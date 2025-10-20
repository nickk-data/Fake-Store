create or replace table fake_dataset.fake_products (
	product_id int64,
	price decimal(6,2),
	description string,
	category string,
	rating decimal(2,1),
	rating_count int64,
	insert_date_time timestamp
);

-- BigQuery supports primary key declaration, but it does not enforce uniqueness or non-null constraints on the data, so I opted to not include them when setting up the table.
