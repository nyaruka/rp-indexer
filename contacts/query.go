package contacts

import (
	"database/sql"
	"time"
)

const sqlSelectModified = `
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
			SELECT array_to_json(array_agg(g.uuid)) FROM (
				SELECT contacts_contactgroup.uuid
				FROM contacts_contactgroup_contacts, contacts_contactgroup
				WHERE contact_id = contacts_contact.id AND contacts_contactgroup_contacts.contactgroup_id = contacts_contactgroup.id
			) g
		) AS groups,
		(
			SELECT f.uuid FROM flows_flow f WHERE f.id = contacts_contact.current_flow_id
		) AS flow
	FROM contacts_contact
	WHERE modified_on >= $1
	ORDER BY modified_on ASC
	LIMIT 500000
) t;
`

func FetchModified(db *sql.DB, lastModified time.Time) (*sql.Rows, error) {
	return db.Query(sqlSelectModified, lastModified)
}
