CREATE TABLE public.dim_date
(
    dateid INT NOT NULL,
    date DATE,
    year INT,
    quarter INT,
    quartername VARCHAR(2),
    month INT,
    monthname VARCHAR(50),
    day INT,
    weekday INT,
    weekdayname VARCHAR(50),
    CONSTRAINT dim_date_pkey PRIMARY KEY (dateid)
);

CREATE TABLE public.dim_category
(
    categoryid INT NOT NULL,
    category VARCHAR(50),
    CONSTRAINT dim_category_pkey PRIMARY KEY (categoryid)
);

CREATE TABLE public.dim_country
(
    countryid INT NOT NULL,
    country VARCHAR(50),
    CONSTRAINT dim_country_pkey PRIMARY KEY (countryid)
);

CREATE TABLE public.fact_orders
(
    orderid INT NOT NULL,
    dateid INT,
    countryid INT,
    categoryid INT,
    amount INT,
    CONSTRAINT fact_orders_pkey PRIMARY KEY (orderid)
);
