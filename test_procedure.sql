
CREATE OR REPLACE PROCEDURE update_supplier_contact (
    p_supplier_id FLOAT,
    p_new_contact_name VARCHAR,
    p_new_contact_email VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
try {
    snowflake.execute({sqlText: `UPDATE SUPPLIERS SET CONTACT_NAME = ?, CONTACT_EMAIL = ? WHERE SUPPLIER_ID = ?`, binds: [P_NEW_CONTACT_NAME, P_NEW_CONTACT_EMAIL, P_SUPPLIER_ID]});
    return 'Updated Successfully';
  }
  catch (error) {
    return 'Error: ' + error.message;
  }
$$
;

CALL update_supplier_contact(103, 'Eswar', 'david.wright@audiopro.com');

