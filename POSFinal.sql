-- ETL

DROP DATABASE if exists pos;

CREATE DATABASE pos;

CREATE TABLE pos.tempCust(
	id int not null,
	firstName text,
	lastName text,
	email varchar(128),
	address varchar(128),
	city varchar(30),
	`state` varchar(30),
	zipCode int not null,	
	birthDate varchar(20),
	Primary key(id)
)Engine = InnoDB;

LOAD DATA LOCAL INFILE '/home/dgomillion/Customer.csv'
INTO TABLE pos.tempCust
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
ignore 1 rows
(id,firstName,lastName,email,address,city,state,zipCode,@var_birthDate)
SET birthDate = STR_TO_DATE(@var_birthDate,'%m/%d/%Y');

CREATE TABLE pos.Zip(
	zip int not null,
	city varchar(30),
	`state` varchar(30),
	PRIMARY KEY(zip)
)Engine = InnoDB;

INSERT INTO pos.Zip SELECT DISTINCT zipCode,city,`state` FROM pos.tempCust;

CREATE TABLE pos.Customer(
	id int not null,
	firstName text,
	lastName text,
	email varchar(100),
	address varchar(100),
	birthDate varchar(25),	
	zip int,
	Primary key(id),
	Foreign key(zip) references pos.Zip(zip)
)Engine = InnoDB;

INSERT INTO pos.Customer SELECT id,firstName,lastName,email,address,birthDate,zipCode FROM pos.tempCust;

CREATE TABLE pos.Order(
	id int not null,
	customerID int not null,
	Primary key (id),
	Foreign key (customerID) references pos.Customer(id)
)Engine = InnoDB;

LOAD DATA LOCAL INFILE '/home/dgomillion/Order.csv'
INTO TABLE pos.Order
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
(id, customerID);

CREATE TABLE pos.Product(
	id int not null,
	app varchar(50),
	price decimal(4,2),
	Primary key(id)
)Engine = InnoDB;

LOAD DATA LOCAL INFILE '/home/dgomillion/Product.csv'
INTO TABLE pos.Product
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
ignore 1 rows
(id,app,@var_price)
SET price = REPLACE(@var_price,'$','');


CREATE TABLE pos.tempOrderLine(
	orderID int,
	productID int
)Engine = InnoDB;

LOAD DATA LOCAL INFILE '/home/dgomillion/OrderLine.csv'
INTO TABLE pos.tempOrderLine
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
(orderID,productID);

CREATE TABLE pos.OrderLine(
	orderID int not null,
	productID int not null,
	quantity int,
	Primary key (orderID,productID),
	Foreign key (orderID) references pos.Order(id),
	FOREIGN KEY (productID) references pos.Product(id)
)Engine = InnoDB;
	
INSERT INTO pos.OrderLine SELECT orderID,productID,count(*) FROM pos.tempOrderLine GROUP BY orderID,productID;

DROP TABLE pos.tempOrderLine;
DROP TABLE pos.tempCust;

-- VIEWS

use pos;

CREATE OR REPLACE VIEW v_Customers
AS SELECT c.lastName, c.firstName, c.email, c.address, z.city, z.state, z.zip
FROM Customer c left join Zip z 
ON c.zip = z.zip
ORDER BY c.lastName ASC, c.firstName ASC, c.birthDate ASC; 

CREATE OR REPLACE VIEW v_CustomerProducts 
AS SELECT  c.lastName, c.firstName, GROUP_CONCAT(DISTINCT(p.app) ORDER BY p.app SEPARATOR ',') AS apps
FROM Customer c
left join `Order` o ON  c.id = o.customerID
left join  OrderLine ol ON o.id = ol.orderId 
left join Product p on p.id = 	ol.productID
group by c.id
order by c.lastName, c.firstName;

CREATE OR REPLACE VIEW v_ProductCustomers
as select p.app as app, p.id as productID, GROUP_CONCAT(DISTINCT CONCAT(c.firstName,' ', c.lastName) ORDER BY c.lastName, c.firstName SEPARATOR ',' ) as customers
from Product p 
left join OrderLine ol ON p.id = ol.productID
left join `Order` o ON o.id = ol.orderID 
left join Customer c ON c.id = o.customerID
GROUP BY p.id;

CREATE OR REPLACE TABLE mv_ProductCustomers(
	  app Text, 
	  productID Int Primary Key, 
	  customers Text 
) ENGINE = InnoDB;

INSERT INTO mv_ProductCustomers
SELECT p.app AS app, p.id AS productID, GROUP_CONCAT(DISTINCT CONCAT(c.firstName,' ', c.lastName) ORDER BY c.lastName, c.firstName SEPARATOR ',' ) as customers
FROM Product p 
LEFT JOIN OrderLine ol ON p.id = ol.productID
LEFT JOIN `Order` o ON o.id = ol.orderID 
LEFT JOIN Customer c ON c.id = o.customerID
GROUP BY p.id;

-- TRANSACTION

use pos;
START TRANSACTION;
SET autocommit = 0;
INSERT INTO pos.Customer VALUES(99999, 'Rohini', 'Joshi', 'rohini.joshi@tamu.edu', 'Reveille Ranch', STR_TO_DATE("06/21/1998", '%m/%d/%Y'), 90101);
INSERT INTO pos.`Order` VALUES(99999, 99999);
INSERT INTO pos.OrderLine VALUES(99999,17,1);
INSERT INTO pos.OrderLine VALUES(99999,27,1);
INSERT INTO pos.OrderLine VALUES(99999,57,1);
COMMIT;

START TRANSACTION;
SET autocommit = 0;
INSERT INTO pos.Customer VALUES(99998, 'Aayush', 'Sharma', 'aayush.sharma582@tamu.edu', 'Reveille Ranch', STR_TO_DATE("08/23/1995", '%m/%d/%Y'), 65805);
INSERT INTO pos.`Order` VALUES(99998, 99997);
INSERT INTO pos.OrderLine VALUES(99998,18,2);
INSERT INTO pos.OrderLine VALUES(99998,28,2);
INSERT INTO pos.OrderLine VALUES(99998,58,2);
COMMIT;

-- INDEX

use pos;
CREATE OR REPLACE INDEX appindex ON pos.Product(app);
CREATE OR REPLACE FULLTEXT INDEX ftindex ON pos.mv_ProductCustomers(customers);

-- STORED PROCEDURE

use pos;
ALTER TABLE pos.OrderLine
ADD OrderLine.unitPrice decimal(7,2);

ALTER TABLE pos.OrderLine
ADD OrderLine.totalPrice decimal(7,2);

ALTER TABLE pos.`Order`
ADD `Order`.totalPrice decimal(7,2);

DELIMITER //
CREATE OR REPLACE PROCEDURE spCalculateTotals()
BEGIN 

UPDATE pos.OrderLine
JOIN pos.Product ON Product.id = OrderLine.productID
SET OrderLine.unitPrice = (SELECT Product.price FROM pos.Product WHERE OrderLine.productID = Product.id)
WHERE OrderLine.unitPrice is NULL;

UPDATE pos.OrderLine
SET OrderLine.totalPrice = OrderLine.unitPrice * OrderLine.quantity WHERE OrderLine.totalPrice is NULL;

UPDATE pos.`Order`
JOIN pos.OrderLine ON `Order`.id = OrderLine.orderID
SET `Order`.totalPrice =(SELECT sum(OrderLine.totalPrice) FROM pos.OrderLine WHERE OrderLine.orderID = `Order`.id GROUP BY OrderLine.orderID)
WHERE `Order`.totalPrice is NULL;

END; //
DELIMITER ;


DELIMITER //
CREATE OR REPLACE PROCEDURE spCalculateTotalsLoop()
BEGIN 

DECLARE `oid` INT;
DECLARE  pid INT;
DECLARE pri DECIMAL(7,2);
DECLARE done BOOLEAN DEFAULT FALSE;

DECLARE olcur CURSOR FOR SELECT OrderLine.orderID, OrderLine.productID, Product.price
FROM OrderLine JOIN Product ON OrderLine.productID = Product.id;

DECLARE ocur CURSOR FOR SELECT id FROM `Order`;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

OPEN olcur;

unitprice_loop: LOOP

FETCH olcur INTO `oid`,pid,pri;
IF done THEN 
LEAVE unitprice_loop;
END IF;

UPDATE OrderLine SET unitPrice = pri WHERE productID = pid and orderID = `oid`;
UPDATE OrderLine SET totalPrice = quantity*pri WHERE productID = pid and orderID = `oid`;
END LOOP unitprice_loop;
CLOSE olcur;
SET done = FALSE;

OPEN ocur;

order_loop: LOOP
FETCH ocur INTO `oid`;
IF done THEN 
LEAVE order_loop;
END IF;

UPDATE `Order` SET totalPrice = (SELECT SUM(totalPrice) FROM OrderLine WHERE orderID = `oid` GROUP BY orderID) WHERE `Order`.id = `oid`;
END LOOP order_loop;
CLOSE ocur;
END; //

DELIMITER ;

DELIMITER //

CREATE OR REPLACE PROCEDURE spFillMVProductCustomers()
BEGIN 
DELETE FROM mv_ProductCustomers;

INSERT INTO mv_ProductCustomers
SELECT p.app AS app, p.id AS productID, GROUP_CONCAT(DISTINCT CONCAT(c.firstName,' ', c.lastName) ORDER BY c.lastName, c.firstName SEPARATOR ',' ) as customers
FROM Product p 
LEFT JOIN OrderLine ol ON p.id = ol.productID
LEFT JOIN `Order` o ON o.id = ol.orderID 
LEFT JOIN Customer c ON c.id = o.customerID
GROUP BY p.id;

END //
DELIMITER ;

-- TRIGGERS

use pos;

CALL spCalculateTotals();
CALL spFillMVProductCustomers();


DELIMITER //
CREATE OR REPLACE 
TRIGGER binsert_ol BEFORE INSERT
ON pos.OrderLine FOR EACH ROW 
BEGIN
    SET NEW.unitPrice = (SELECT price from pos.Product P where P.id = NEW.productID);
    SET NEW.totalPrice = NEW.unitPrice * NEW.quantity;
END;//
DELIMITER ;


DELIMITER //
CREATE OR REPLACE 
TRIGGER bupdate_ol BEFORE UPDATE
ON pos.OrderLine FOR EACH ROW 
BEGIN
    SET NEW.unitPrice = (SELECT price from pos.Product P where P.id = NEW.productID);
    SET NEW.totalPrice = NEW.unitPrice * NEW.quantity;
END;//
DELIMITER ;


DELIMITER //
CREATE OR REPLACE 
TRIGGER ainsert_ol AFTER INSERT
ON pos.OrderLine FOR EACH ROW 
BEGIN
    UPDATE pos.Order o
    SET o.totalPrice = (SELECT SUM(ol.totalPrice) FROM pos.OrderLine ol WHERE ol.orderID = o.id GROUP BY ol.orderID) WHERE o.id = NEW.orderID;

    DELETE FROM pos.mv_ProductCustomers;
    INSERT INTO pos.mv_ProductCustomers SELECT app, productID, customers FROM pos.v_ProductCustomers;
END;//
DELIMITER ;


DELIMITER //
CREATE OR REPLACE 
TRIGGER aupdate_ol AFTER UPDATE
ON pos.OrderLine FOR EACH ROW 
BEGIN
    UPDATE pos.Order o
    SET o.totalPrice = (SELECT SUM(ol.totalPrice) FROM pos.OrderLine ol WHERE ol.orderID = o.id GROUP BY ol.orderID) WHERE o.id = NEW.orderID;

    DELETE FROM pos.mv_ProductCustomers;
    INSERT INTO pos.mv_ProductCustomers SELECT app, productID, customers FROM pos.v_ProductCustomers;
END;//
DELIMITER ;


DELIMITER //
CREATE OR REPLACE 
TRIGGER adelete_ol AFTER DELETE
ON pos.OrderLine FOR EACH ROW 
BEGIN
    UPDATE pos.Order o
    SET o.totalPrice = (SELECT SUM(ol.totalPrice) FROM pos.OrderLine ol WHERE ol.orderID = o.id GROUP BY ol.orderID) WHERE o.id = OLD.orderID;

    DELETE FROM pos.mv_ProductCustomers;
    INSERT INTO pos.mv_ProductCustomers SELECT app, productID, customers FROM pos.v_ProductCustomers;
END;//
DELIMITER ;


DELIMITER //
CREATE OR REPLACE 
TRIGGER ainsert_order AFTER INSERT
ON pos.Order FOR EACH ROW 
BEGIN
    DELETE FROM pos.mv_ProductCustomers;
    INSERT INTO pos.mv_ProductCustomers SELECT app, productID, customers FROM pos.v_ProductCustomers;
END;//
DELIMITER ;


DELIMITER //
CREATE OR REPLACE 
TRIGGER aupdate_order AFTER UPDATE
ON pos.Order FOR EACH ROW 
BEGIN
    DELETE FROM pos.mv_ProductCustomers;
    INSERT INTO pos.mv_ProductCustomers SELECT app, productID, customers FROM pos.v_ProductCustomers;
END;//
DELIMITER ;


DELIMITER //
CREATE OR REPLACE 
TRIGGER adelete_order AFTER DELETE
ON pos.Order FOR EACH ROW 
BEGIN
    DELETE FROM pos.mv_ProductCustomers;
    INSERT INTO pos.mv_ProductCustomers SELECT app, productID, customers FROM pos.v_ProductCustomers;
END;//
DELIMITER ;


DELIMITER //
CREATE OR REPLACE 
TRIGGER ainsert_product AFTER INSERT
ON pos.Product FOR EACH ROW 
BEGIN
    DELETE FROM pos.mv_ProductCustomers;
    INSERT INTO pos.mv_ProductCustomers SELECT app, productID, customers FROM pos.v_ProductCustomers;
END;//
DELIMITER ;


DELIMITER //
CREATE OR REPLACE 
TRIGGER adelete_product AFTER DELETE
ON pos.Product FOR EACH ROW 
BEGIN
    DELETE FROM pos.mv_ProductCustomers;
    INSERT INTO pos.mv_ProductCustomers SELECT app, productID, customers FROM pos.v_ProductCustomers;
END;//
DELIMITER ;


CREATE or REPLACE TABLE HistoricalPricing (
    `id` INTEGER NOT NULL auto_increment PRIMARY KEY,
    `productID` INTEGER NOT NULL,
    `changeTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `oldPrice` DECIMAL(5,2),
    `newPrice` DECIMAL(5,2),
    CONSTRAINT foreignKeyproductID FOREIGN KEY(`productID`) REFERENCES Product(`id`) ON DELETE RESTRICT
    )Engine = InnoDB;



DELIMITER //
CREATE OR REPLACE 
TRIGGER aupdate_product AFTER UPDATE 
ON pos.Product FOR EACH ROW 
BEGIN
    DELETE FROM pos.mv_ProductCustomers;
    INSERT INTO pos.mv_ProductCustomers SELECT app, productID, customers FROM pos.v_ProductCustomers;
    INSERT INTO `pos`.`HistoricalPricing`(productID, `oldPrice`, `newPrice`) VALUES (NEW.id, OLD.price, NEW.price);
END;//
DELIMITER ;



	