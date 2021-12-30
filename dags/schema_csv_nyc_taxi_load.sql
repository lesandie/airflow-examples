CREATE UNLOGGED TABLE csv_load (vendor_id smallint, pickup_datetime timestamp without timezone,
					   	dropoff_datetime timestamp without timezone, passenger_count smallint, trip_distance real, 
					   	ratecode_id smallint, store_and_fwd_flag char(1),
					   	pu_location_id smallint, do_location_id smallint, payment_type smallint, fare_amount real,
					   	extra real, mta_tax real, tip_amount real, tolls_amount real, improvement_surcharge real,
						total_amount real, congestion_surcharge real
					  );