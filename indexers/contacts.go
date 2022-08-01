package indexers

import (
	"bytes"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

//go:embed contacts.settings.json
var contactsSettings json.RawMessage

// ContactIndexer is an indexer for contacts
type ContactIndexer struct {
	baseIndexer

	batchSize int
}

// NewContactIndexer creates a new contact indexer
func NewContactIndexer(elasticURL, name string, batchSize int) *ContactIndexer {
	return &ContactIndexer{
		baseIndexer: newBaseIndexer(elasticURL, name),
		batchSize:   batchSize,
	}
}

// Index indexes modified contacts and returns the name of the concrete index
func (i *ContactIndexer) Index(db *sql.DB, rebuild, cleanup bool) (string, error) {
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
		physicalIndex, err = i.createNewIndex(contactsSettings)
		if err != nil {
			return "", errors.Wrap(err, "error creating new index")
		}
		i.log().WithField("index", physicalIndex).Info("created new physical index")
		remapAlias = true
	}

	lastModified, err := i.GetLastModified(physicalIndex)
	if err != nil {
		return "", errors.Wrap(err, "error finding last modified")
	}

	i.log().WithField("index", physicalIndex).WithField("last_modified", lastModified).Debug("indexing newer than last modified")

	// now index our docs
	start := time.Now()
	indexed, deleted, err := i.indexModified(db, physicalIndex, lastModified.Add(-5*time.Second), rebuild)
	if err != nil {
		return "", errors.Wrap(err, "error indexing documents")
	}

	i.recordComplete(indexed, deleted, time.Since(start))

	// if the index didn't previously exist or we are rebuilding, remap to our alias
	if remapAlias {
		err := i.updateAlias(physicalIndex)
		if err != nil {
			return "", errors.Wrap(err, "error updating alias")
		}
		remapAlias = false
	}

	// cleanup our aliases if appropriate
	if cleanup {
		err := i.cleanupIndexes()
		if err != nil {
			return "", errors.Wrap(err, "error cleaning up old indexes")
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
			SELECT array_to_json(array_agg(gc.contactgroup_id)) FROM contacts_contactgroup_contacts gc WHERE gc.contact_id = contacts_contact.id
		) AS group_ids,
		current_flow_id AS flow_id,
		(
			SELECT array_to_json(array_agg(DISTINCT fr.flow_id)) FROM flows_flowrun fr WHERE fr.contact_id = contacts_contact.id
		) AS flow_history_ids
	FROM contacts_contact
	WHERE modified_on >= $1
	ORDER BY modified_on ASC
	LIMIT 500000
) t;
`

// IndexModified queries and indexes all contacts with a lastModified greater than or equal to the passed in time
func (i *ContactIndexer) indexModified(db *sql.DB, index string, lastModified time.Time, rebuild bool) (int, int, error) {
	totalFetched, totalCreated, totalDeleted := 0, 0, 0

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
		batchDeleted := 0               // contacts deleted in ES
		batchESTime := time.Duration(0) // time spent indexing for this batch

		indexSubBatch := func(b *bytes.Buffer) error {
			t := time.Now()
			created, deleted, err := i.indexBatch(index, b.Bytes())
			if err != nil {
				return err
			}

			batchESTime += time.Since(t)
			batchCreated += created
			batchDeleted += deleted
			b.Reset()
			return nil
		}

		rows, err := db.Query(sqlSelectModifiedContacts, lastModified)

		queryModified := lastModified

		// no more rows? return
		if err == sql.ErrNoRows {
			return 0, 0, nil
		}
		if err != nil {
			return 0, 0, err
		}
		defer rows.Close()

		for rows.Next() {
			err = rows.Scan(&orgID, &id, &modifiedOn, &isActive, &contactJSON)
			if err != nil {
				return 0, 0, err
			}

			batchFetched++
			lastModified = modifiedOn

			if isActive {
				logrus.WithField("id", id).WithField("modifiedOn", modifiedOn).WithField("contact", contactJSON).Debug("modified contact")

				subBatch.WriteString(fmt.Sprintf(indexCommand, id, modifiedOn.UnixNano(), orgID))
				subBatch.WriteString("\n")
				subBatch.WriteString(contactJSON)
				subBatch.WriteString("\n")
			} else {
				logrus.WithField("id", id).WithField("modifiedOn", modifiedOn).Debug("deleted contact")

				subBatch.WriteString(fmt.Sprintf(deleteCommand, id, modifiedOn.UnixNano(), orgID))
				subBatch.WriteString("\n")
			}

			// write to elastic search in batches
			if batchFetched%i.batchSize == 0 {
				if err := indexSubBatch(subBatch); err != nil {
					return 0, 0, err
				}
			}
		}

		if subBatch.Len() > 0 {
			if err := indexSubBatch(subBatch); err != nil {
				return 0, 0, err
			}
		}

		rows.Close()

		totalFetched += batchFetched
		totalCreated += batchCreated
		totalDeleted += batchDeleted

		totalTime := time.Since(start)
		batchTime := time.Since(batchStart)
		batchRate := int(float32(batchFetched) / (float32(batchTime) / float32(time.Second)))

		log := i.log().WithField("index", index).WithFields(logrus.Fields{
			"rate":             batchRate,
			"batch_fetched":    batchFetched,
			"batch_created":    batchCreated,
			"batch_elapsed":    batchTime,
			"batch_elapsed_es": batchESTime,
			"total_fetched":    totalFetched,
			"total_created":    totalCreated,
			"total_elapsed":    totalTime,
		})

		// if we're rebuilding, always log batch progress
		if rebuild {
			log.Info("indexed contact batch")
		} else {
			log.Debug("indexed contact batch")
		}

		// last modified stayed the same and we didn't add anything, seen it all, break out
		if lastModified.Equal(queryModified) && batchCreated == 0 {
			break
		}
	}

	return totalCreated, totalDeleted, nil
}
