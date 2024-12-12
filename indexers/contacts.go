package indexers

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"time"

	"github.com/nyaruka/rp-indexer/v9/runtime"
)

//go:embed contacts.index.json
var contactsIndexDef []byte

// ContactIndexer is an indexer for contacts
type ContactIndexer struct {
	baseIndexer

	batchSize int
}

// NewContactIndexer creates a new contact indexer
func NewContactIndexer(elasticURL, name string, shards, replicas, batchSize int) *ContactIndexer {
	def := newIndexDefinition(contactsIndexDef, shards, replicas)

	return &ContactIndexer{
		baseIndexer: newBaseIndexer(elasticURL, name, def),
		batchSize:   batchSize,
	}
}

// Index indexes modified contacts and returns the name of the concrete index
func (i *ContactIndexer) Index(rt *runtime.Runtime, rebuild, cleanup bool) (string, error) {
	ctx := context.TODO()
	var err error

	// find our physical index
	physicalIndexes := i.FindIndexes()

	physicalIndex := ""
	if len(physicalIndexes) > 0 {
		physicalIndex = physicalIndexes[0]
	}

	// whether we need to remap our alias after building
	remapAlias := false

	// doesn't exist or we are rebuilding, create it
	if physicalIndex == "" || rebuild {
		physicalIndex, err = i.createNewIndex(i.definition)
		if err != nil {
			return "", fmt.Errorf("error creating new index: %w", err)
		}
		i.log().Info("created new physical index", "index", physicalIndex)
		remapAlias = true
	}

	lastModified, err := i.GetESLastModified(physicalIndex)
	if err != nil {
		return "", fmt.Errorf("error finding last modified: %w", err)
	}

	i.log().Debug("indexing newer than last modified", "index", physicalIndex, "last_modified", lastModified)

	// now index our docs
	err = i.indexModified(ctx, rt.DB, physicalIndex, lastModified.Add(-5*time.Second), rebuild)
	if err != nil {
		return "", fmt.Errorf("error indexing documents: %w", err)
	}

	// if the index didn't previously exist or we are rebuilding, remap to our alias
	if remapAlias {
		err := i.updateAlias(physicalIndex)
		if err != nil {
			return "", fmt.Errorf("error updating alias: %w", err)
		}
		remapAlias = false
	}

	// cleanup our aliases if appropriate
	if cleanup {
		err := i.cleanupIndexes()
		if err != nil {
			return "", fmt.Errorf("error cleaning up old indexes: %w", err)
		}
	}

	return physicalIndex, nil
}

const sqlSelectModifiedContacts = `
SELECT org_id, id, modified_on, is_active, row_to_json(t) FROM (
	SELECT
		id,
		org_id,
		uuid,
		name,
		language,
		status,
		ticket_count AS tickets,
		is_active,
		created_on,
		modified_on,
		last_seen_on,
		EXTRACT(EPOCH FROM modified_on) * 1000000 AS modified_on_mu,
		(
			SELECT array_to_json(array_agg(row_to_json(u)))
			FROM (SELECT scheme, path FROM contacts_contacturn WHERE contact_id = contacts_contact.id) u
		) AS urns,
		(
			SELECT jsonb_agg(f.value)
			FROM (
				SELECT 
					CASE
					WHEN value ? 'ward'
					THEN jsonb_build_object('ward_keyword', trim(substring(value ->> 'ward' from  '(?!.* > )([^>]+)')))
					ELSE '{}'::jsonb
					END || district_value.value AS value
				FROM (
					SELECT 
						CASE
						WHEN value ? 'district'
						THEN jsonb_build_object('district_keyword', trim(substring(value ->> 'district' from  '(?!.* > )([^>]+)')))
						ELSE '{}'::jsonb
						END || state_value.value as value
					FROM (
						SELECT 
							CASE
							WHEN value ? 'state'
							THEN jsonb_build_object('state_keyword', trim(substring(value ->> 'state' from  '(?!.* > )([^>]+)')))
							ELSE '{}' :: jsonb
							END || jsonb_build_object('field', key) || value as value
						FROM jsonb_each(contacts_contact.fields)
					) state_value
				) AS district_value
			) AS f
		) AS fields,
		(
			SELECT array_to_json(array_agg(gc.contactgroup_id)) 
			FROM contacts_contactgroup_contacts gc 
			INNER JOIN contacts_contactgroup g ON g.id = gc.contactgroup_id
			WHERE gc.contact_id = contacts_contact.id AND g.group_type IN ('M', 'Q')
		) AS group_ids,
		current_flow_id AS flow_id,
		(
			SELECT array_to_json(array_agg(DISTINCT fr.flow_id)) FROM flows_flowrun fr WHERE fr.contact_id = contacts_contact.id
		) AS flow_history_ids
	FROM contacts_contact
	WHERE modified_on >= $1
	ORDER BY modified_on ASC
	LIMIT 100000
) t;
`

// IndexModified queries and indexes all contacts with a lastModified greater than or equal to the passed in time
func (i *ContactIndexer) indexModified(ctx context.Context, db *sql.DB, index string, lastModified time.Time, rebuild bool) error {
	totalFetched, totalCreated, totalUpdated, totalDeleted := 0, 0, 0, 0

	var modifiedOn time.Time
	var contactJSON string
	var id, orgID int64
	var isActive bool

	subBatch := &bytes.Buffer{}
	start := time.Now()

	for {
		batchStart := time.Now()        // start time for this batch
		batchFetched := 0               // contacts fetched in this batch
		batchCreated := 0               // contacts created in ES
		batchUpdated := 0               // contacts updated in ES
		batchDeleted := 0               // contacts deleted in ES
		batchESTime := time.Duration(0) // time spent indexing for this batch

		indexSubBatch := func(b *bytes.Buffer) error {
			t := time.Now()
			created, updated, deleted, err := i.indexBatch(index, b.Bytes())
			if err != nil {
				return err
			}

			batchESTime += time.Since(t)
			batchCreated += created
			batchUpdated += updated
			batchDeleted += deleted
			b.Reset()
			return nil
		}

		rows, err := db.QueryContext(ctx, sqlSelectModifiedContacts, lastModified)

		queryModified := lastModified

		// no more rows? return
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			err = rows.Scan(&orgID, &id, &modifiedOn, &isActive, &contactJSON)
			if err != nil {
				return err
			}

			batchFetched++
			lastModified = modifiedOn

			if isActive {
				i.log().Debug("modified contact", "id", id, "modifiedOn", modifiedOn, "contact", contactJSON)

				subBatch.WriteString(fmt.Sprintf(indexCommand, id, modifiedOn.UnixNano(), orgID))
				subBatch.WriteString("\n")
				subBatch.WriteString(contactJSON)
				subBatch.WriteString("\n")
			} else {
				i.log().Debug("deleted contact", "id", id, "modifiedOn", modifiedOn)

				subBatch.WriteString(fmt.Sprintf(deleteCommand, id, modifiedOn.UnixNano(), orgID))
				subBatch.WriteString("\n")
			}

			// write to elastic search in batches
			if batchFetched%i.batchSize == 0 {
				if err := indexSubBatch(subBatch); err != nil {
					return err
				}
			}
		}

		if subBatch.Len() > 0 {
			if err := indexSubBatch(subBatch); err != nil {
				return err
			}
		}

		rows.Close()

		totalFetched += batchFetched
		totalCreated += batchCreated
		totalUpdated += batchUpdated
		totalDeleted += batchDeleted

		totalTime := time.Since(start)
		batchTime := time.Since(batchStart)
		batchRate := int(float32(batchFetched) / (float32(batchTime) / float32(time.Second)))

		log := i.log().With("index", index,
			"rate", batchRate,
			"batch_fetched", batchFetched,
			"batch_created", batchCreated,
			"batch_updated", batchUpdated,
			"batch_elapsed", batchTime,
			"batch_elapsed_es", batchESTime,
			"total_fetched", totalFetched,
			"total_created", totalCreated,
			"total_updated", totalUpdated,
			"total_elapsed", totalTime,
		)

		// if we're rebuilding, always log batch progress
		if rebuild {
			log.Info("indexed contact batch")
		} else {
			log.Debug("indexed contact batch")
		}

		i.recordActivity(batchCreated+batchUpdated, batchDeleted, time.Since(batchStart))

		// last modified stayed the same and we didn't add anything, seen it all, break out
		if lastModified.Equal(queryModified) && batchCreated == 0 {
			break
		}
	}

	return nil
}

func (i *ContactIndexer) GetDBLastModified(ctx context.Context, db *sql.DB) (time.Time, error) {
	lastModified := time.Time{}

	if err := db.QueryRowContext(ctx, "SELECT MAX(modified_on) FROM contacts_contact").Scan(&lastModified); err != nil {
		return lastModified, err
	}

	return lastModified, nil
}
