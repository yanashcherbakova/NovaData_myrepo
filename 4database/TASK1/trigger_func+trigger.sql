CREATE OR REPLACE FUNCTION log_user_upd()
RETURNS TRIGGER AS $$
	DECLARE
		bd_user text := current_user;
	BEGIN
		IF 
			OLD.name IS DISTINCT FROM NEW.name THEN 
			INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
			VALUES (OLD.id, bd_user, 'name', OLD.name, NEW.name);
		END IF;

		IF
			OLD.email IS DISTINCT FROM NEW.email THEN
			INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
			VALUES (OLD.id, bd_user, 'email', OLD.email, NEW.email);
		END IF;

		IF
			OLD.role IS DISTINCT FROM NEW.role THEN
			INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
			VALUES (OLD.id, bd_user, 'role', OLD.role, NEW.role);
		END IF;
	RETURN NEW;
	END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER logger_upd
AFTER UPDATE ON users
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION log_user_upd();
