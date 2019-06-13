-- update one of our contacts
DELETE FROM contacts_contactgroup_contacts WHERE id = 3;
UPDATE contacts_contact SET name = 'John Deer', modified_on = '2020-08-20 14:00:00+00' where id = 2;

-- delete one of our others
UPDATE contacts_contact SET is_active = FALSE, modified_on = '2020-08-22 15:00:00+00' where id = 4;


