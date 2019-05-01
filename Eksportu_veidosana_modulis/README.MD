<b>Table creation scripts:</b>

CREATE TABLE IF NOT EXISTS client_data.driver(
    driverID string,
    email string,
    second_name string,
    first_name string,
    driver_licence_ISS string,
    driver_licence_EXP string
);

INSERT INTO TABLE client_data.driver VALUES
('0000000001', 'FaithStone@dayrep.com', 'Stone', 'Faith', '02/05/15', '02/05/20'),
('0000000002', 'SarahHope@armyspy.com', 'Sarah', 'Hope', '20/10/15', '20/10/20'),
('0000000003', 'AlexanderBaxter@dayrep.com', 'Alexander', 'Baxter ', '02/05/15', '02/05/20'),
('0000000004', 'LeahWoods@dayrep.com', 'Leah', 'Woods', '02/05/15', '02/05/20'),
('0000000005', 'MuhammadMetcalfe@dayrep.com', 'Muhammad', 'Metcalfe', '02/05/15', '02/05/20'),
('0000000006', 'MorganPatterson@armyspy.com', 'Morgan', 'Patterson', '02/05/15', '02/05/20');

+------------------+------------------------------+---------------------+--------------------+----------------------------+----------------------------+
| driver.driverid  |         driver.email         | driver.second_name  | driver.first_name  | driver.driver_licence_iss  | driver.driver_licence_exp  |
+------------------+------------------------------+---------------------+--------------------+----------------------------+----------------------------+
| 0000000001       | FaithStone@dayrep.com        | Stone               | Faith              | 02/05/15                   | 02/05/20                   |
| 0000000002       | SarahHope@armyspy.com        | Sarah               | Hope               | 20/10/15                   | 20/10/20                   |
| 0000000003       | AlexanderBaxter@dayrep.com   | Alexander           | Baxter             | 02/05/15                   | 02/05/20                   |
| 0000000004       | LeahWoods@dayrep.com         | Leah                | Woods              | 02/05/15                   | 02/05/20                   |
| 0000000005       | MuhammadMetcalfe@dayrep.com  | Muhammad            | Metcalfe           | 02/05/15                   | 02/05/20                   |
| 0000000006       | MorganPatterson@armyspy.com  | Morgan              | Patterson          | 02/05/15                   | 02/05/20                   |
+------------------+------------------------------+---------------------+--------------------+----------------------------+----------------------------+

CREATE TABLE IF NOT EXISTS client_data.client(
    clientID string,
    email string,
    second_name string,
    first_name string
);

INSERT INTO TABLE client_data.client VALUES
('0000000007', 'KrisJune@dayrep.com', 'Kris', 'June'),
('0000000008', 'EveretteGayla@armyspy.com', 'Everette', 'Gayla'),
('0000000009', 'GabrielMary@dayrep.com', 'Gabriel', 'Mary'),
('0000000010', 'NanceMeghan@dayrep.com', 'Nance', 'Woods'),
('0000000011', 'JeniferChloe@dayrep.com', 'Jenifer', 'Chloe');

+------------------+----------------------------+---------------------+--------------------+
| client.clientid  |        client.email        | client.second_name  | client.first_name  |
+------------------+----------------------------+---------------------+--------------------+
| 0000000007       | KrisJune@dayrep.com        | Kris                | June               |
| 0000000008       | EveretteGayla@armyspy.com  | Everette            | Gayla              |
| 0000000009       | GabrielMary@dayrep.com     | Gabriel             | Mary               |
| 0000000010       | NanceMeghan@dayrep.com     | Nance               | Woods              |
| 0000000011       | JeniferChloe@dayrep.com    | Jenifer             | Chloe              |
+------------------+----------------------------+---------------------+--------------------+

CREATE TABLE IF NOT EXISTS client_data.ride(
    clientID string,
    driverID string,
    amount integer,
    duration string,
    distance float,
    send_request_time string,
    ride_start_time string
);

INSERT INTO TABLE client_data.ride VALUES
('0000000001','00000000011', 20, '0.25', 7.1234, '2019-01-05 13:31:15', '2019-01-05 13:35:15'),
('0000000002','00000000010', 21, '0.25',7.1234, '2019-01-05 13:31:15', '2019-01-05 13:35:15'),
('0000000003','00000000009', 23, '0.25',7.1234, '2019-01-05 13:31:15', '2019-01-05 13:35:15'),
('0000000004','00000000009', 5, '0.25', 7.1234,'2019-01-05 13:31:15', '2019-01-05 13:35:15'),
('0000000005','00000000008', 9, '0.25',7.1234, '2019-01-05 13:31:15', '2019-01-05 13:35:15'),
('0000000006','00000000011', 21, '0.25',7.1234, '2019-01-05 13:31:15', '2019-01-05 13:35:15'),
('0000000001','00000000011', 19, '0.25',7.1234, '2019-01-05 13:31:15', '2019-01-05 13:35:15'),
('0000000002','00000000011', 3, '0.25', 7.1234,'2019-01-05 13:31:15', '2019-01-05 13:35:15');

+----------------+----------------+--------------+----------------+----------------+-------------------------+-----------------------+
| ride.clientid  | ride.driverid  | ride.amount  | ride.duration  | ride.distance  | ride.send_request_time  | ride.ride_start_time  |
+----------------+----------------+--------------+----------------+----------------+-------------------------+-----------------------+
| 0000000001     | 00000000011    | 20           | 0.25           | 7.1234         | 2019-01-05 13:31:15     | 2019-01-05 13:35:15   |
| 0000000002     | 00000000010    | 21           | 0.25           | 7.1234         | 2019-01-05 13:31:15     | 2019-01-05 13:35:15   |
| 0000000003     | 00000000009    | 23           | 0.25           | 7.1234         | 2019-01-05 13:31:15     | 2019-01-05 13:35:15   |
| 0000000004     | 00000000009    | 5            | 0.25           | 7.1234         | 2019-01-05 13:31:15     | 2019-01-05 13:35:15   |
| 0000000005     | 00000000008    | 9            | 0.25           | 7.1234         | 2019-01-05 13:31:15     | 2019-01-05 13:35:15   |
| 0000000006     | 00000000011    | 21           | 0.25           | 7.1234         | 2019-01-05 13:31:15     | 2019-01-05 13:35:15   |
| 0000000001     | 00000000011    | 19           | 0.25           | 7.1234         | 2019-01-05 13:31:15     | 2019-01-05 13:35:15   |
| 0000000002     | 00000000011    | 3            | 0.25           | 7.1234         | 2019-01-05 13:31:15     | 2019-01-05 13:35:15   |
+----------------+----------------+--------------+----------------+----------------+-------------------------+-----------------------+

CREATE TABLE IF NOT EXISTS client_data.cars(
      carID string,
      registration_number string,
      type string,
      seats_number integer,
      region string,
      colour string,
      country string
  );

INSERT INTO TABLE client_data.cars VALUES
('0000000001','7BRF690', 'economy', 4, 'California', 'black', 'USA'),
('0000000002','8BRF690', 'economy', 4, 'California', 'black', 'USA'),
('0000000003','9BRF690', 'economy', 4, 'California', 'black', 'USA'),
('0000000004','1BRF690', 'economy', 4, 'California', 'black', 'USA'),
('0000000005','2BRF690', 'economy', 4, 'California', 'black', 'USA'),
('0000000006','3BRF690', 'economy', 4, 'California', 'black', 'USA');

+-------------+---------------------------+------------+--------------------+--------------+--------------+---------------+
| cars.carid  | cars.registration_number  | cars.type  | cars.seats_number  | cars.region  | cars.colour  | cars.country  |
+-------------+---------------------------+------------+--------------------+--------------+--------------+---------------+
| 0000000001  | 7BRF690                   | economy    | 4                  | California   | black        | USA           |
| 0000000002  | 8BRF690                   | economy    | 4                  | California   | black        | USA           |
| 0000000003  | 9BRF690                   | economy    | 4                  | California   | black        | USA           |
| 0000000004  | 1BRF690                   | economy    | 4                  | California   | black        | USA           |
| 0000000005  | 2BRF690                   | economy    | 4                  | California   | black        | USA           |
| 0000000006  | 3BRF690                   | economy    | 4                  | California   | black        | USA           |
+-------------+---------------------------+------------+--------------------+--------------+--------------+---------------+

CREATE TABLE IF NOT EXISTS client_data.driver_cars_mapping(
      carID string,
      driverID string,
      belonging_from string,
      belonging_until string
);



INSERT INTO TABLE client_data.driver_cars_mapping VALUES
('0000000001','0000000001', '2015-01-05', '2021-01-05'),
('0000000002','0000000002', '2015-01-05', '2021-01-05'),
('0000000003','0000000003', '2015-01-05', '2021-01-05'),
('0000000004','0000000004', '2015-01-05', '2021-01-05'),
('0000000005','0000000005', '2015-01-05', '2021-01-05'),
('0000000006','0000000006', '2015-01-05', '2021-01-05');

+----------------------------+-------------------------------+-------------------------------------+--------------------------------------+
| driver_cars_mapping.carid  | driver_cars_mapping.driverid  | driver_cars_mapping.belonging_from  | driver_cars_mapping.belonging_until  |
+----------------------------+-------------------------------+-------------------------------------+--------------------------------------+
| 0000000001                 | 0000000001                    | 2015-01-05                          | 2021-01-05                           |
| 0000000002                 | 0000000002                    | 2015-01-05                          | 2021-01-05                           |
| 0000000003                 | 0000000003                    | 2015-01-05                          | 2021-01-05                           |
| 0000000004                 | 0000000004                    | 2015-01-05                          | 2021-01-05                           |
| 0000000005                 | 0000000005                    | 2015-01-05                          | 2021-01-05                           |
| 0000000006                 | 0000000006                    | 2015-01-05                          | 2021-01-05                           |
+----------------------------+-------------------------------+-------------------------------------+--------------------------------------+