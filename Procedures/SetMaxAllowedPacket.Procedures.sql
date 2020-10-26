DELIMITER $$

--
-- Create procedure `SetMaxAllowedPacket`
--
CREATE PROCEDURE SetMaxAllowedPacket()
BEGIN
	SET GLOBAL max_allowed_packet = 1073741824;
END
$$

DELIMITER ;