DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS sock;

CREATE TABLE suppliers
(
    ID SERIAL PRIMARY KEY,
	name	 VARCHAR(512) NOT NULL
);

CREATE TABLE socks
(
    ID           SERIAL PRIMARY KEY,
	type		 VARCHAR(512) NOT NULL,
	buy_price		 FLOAT(2) NOT NULL,
	supplier_id BIGINT NOT NULL
);

ALTER TABLE socks ADD CONSTRAINT sock_fk1 FOREIGN KEY (supplier_id) REFERENCES suppliers(id);

INSERT INTO suppliers (name)
VALUES ('Socks4U'),
       ('Meltoc'),
       ('Somes');

INSERT INTO socks (type, buy_price, supplier_id)
VALUES ('invisible', 3.79, 1),
       ('low-cute', 1.49, 1),
       ('over the calf', 2.64, 1),
       ('invisible', 6.39, 2),
       ('low-cute', 2.61, 2),
       ('over the calf', 5.05,  2),
       ('invisible', 5.00,  3),
       ('low-cute', 1.99,  3),
       ('over the calf', 4.15, 3);