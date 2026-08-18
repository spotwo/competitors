BEGIN;

ALTER FUNCTION kernel_lab.ensure_inventory_position(
  uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid
) SET search_path = kernel_lab, pg_catalog, pg_temp;

ALTER FUNCTION kernel_lab.allocate_inventory_position(
  uuid, uuid, text, uuid, numeric
) SET search_path = kernel_lab, pg_catalog, pg_temp;

ALTER FUNCTION kernel_lab.transfer_inventory_quantity(
  uuid, uuid, text, uuid, uuid, numeric
) SET search_path = kernel_lab, pg_catalog, pg_temp;

ALTER FUNCTION kernel_lab.assign_serial_to_position(
  uuid, uuid, uuid
) SET search_path = kernel_lab, pg_catalog, pg_temp;

ALTER FUNCTION kernel_lab.relocate_handling_unit(
  uuid, uuid, uuid, uuid
) SET search_path = kernel_lab, pg_catalog, pg_temp;

COMMIT;
