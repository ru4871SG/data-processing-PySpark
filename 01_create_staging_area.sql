CREATE TABLE sales_staging (
    orderid INT,
    amount INT,
    dateid INT,
    countryid INT,
    categoryid INT,
    date DATE,
    year INT,
    quarter INT,
    quartername VARCHAR(2),
    month INT,
    monthname VARCHAR(50),
    day INT,
    weekday INT,
    weekdayname VARCHAR(50),
    category VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE sales_staging_cleaned (
    orderid INT,
    amount INT,
    dateid INT,
    countryid INT,
    categoryid INT,
    date DATE,
    year INT,
    quarter INT,
    quartername VARCHAR(2),
    month INT,
    monthname VARCHAR(50),
    day INT,
    weekday INT,
    weekdayname VARCHAR(50),
    category VARCHAR(50),
    country VARCHAR(50)
);
