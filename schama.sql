CREATE EXTENSION IF NOT EXISTS ltree;

-- public.category definition

-- Drop table

-- DROP TABLE category;

CREATE TABLE category (
	category_id int8 NOT NULL,
	parent_category_id int8 NULL,
	default_name text NOT NULL,
	default_description text NOT NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	"attributes" jsonb NULL,
	url_path text NOT NULL,
	hierarchy_path public.ltree NULL,
	name_translations jsonb NULL,
	description_translations jsonb NULL,
	migration_status_id varchar(100) NULL,
	CONSTRAINT category_pk PRIMARY KEY (category_id),
	CONSTRAINT category_parent_category_id_fk FOREIGN KEY (parent_category_id) REFERENCES category(category_id) ON DELETE SET NULL
);
CREATE INDEX category_default_name_idx ON public.category USING btree (default_name);
CREATE INDEX hierarchy_path_gist_idx ON public.category USING gist (hierarchy_path);
CREATE INDEX hierarchy_path_idx ON public.category USING btree (hierarchy_path);


-- public.product definition

-- Drop table

-- DROP TABLE product;

CREATE TABLE product (
	product_id int8 NOT NULL,
	author_id int8 NOT NULL,
	category_id int8 NULL,
	price_in_cents int8 NOT NULL,
	title jsonb NOT NULL,
	slug jsonb NOT NULL,
	description jsonb NOT NULL,
	main_image jsonb NULL,
	images jsonb NULL,
	assets jsonb NOT NULL,
	product_type text NOT NULL,
	product_status text NOT NULL,
	metadata jsonb NOT NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	last_updated_at timestamptz NULL,
	status varchar NULL,
	CONSTRAINT product_pk PRIMARY KEY (product_id),
	CONSTRAINT product_category_id_fk FOREIGN KEY (category_id) REFERENCES category(category_id) ON DELETE SET NULL
) PARTITION BY LIST (category_id);
CREATE INDEX product_author_id_idx ON public.product USING btree (author_id);
CREATE INDEX product_category_id_idx ON public.product USING btree (category_id);


-- public.product_download definition

-- Drop table

-- DROP TABLE product_download;

CREATE TABLE product_download (
	download_id int8 NOT NULL,
	product_id int8 NOT NULL,
	downloaded_at timestamptz NOT NULL,
	downloaded_at_day_normalized int8 NULL,
	CONSTRAINT product_download_pk UNIQUE (download_id),
	CONSTRAINT product_download_product_fk FOREIGN KEY (product_id) REFERENCES product(product_id)
);
CREATE INDEX product_download_downloaded_at_day_normalized_idx ON public.product_download USING btree (downloaded_at_day_normalized);
CREATE INDEX product_download_downloaded_at_idx ON public.product_download USING brin (downloaded_at);


-- public.product_product_category definition

-- Drop table

-- DROP TABLE product_product_category;

CREATE TABLE product_product_category (
	product_id int8 NOT NULL,
	category_id int8 NOT NULL,
	CONSTRAINT product_product_category_pk PRIMARY KEY (product_id, category_id),
	CONSTRAINT product_product_category_category_id_fk FOREIGN KEY (category_id) REFERENCES category(category_id) ON DELETE CASCADE,
	CONSTRAINT product_product_category_product_id_fk FOREIGN KEY (product_id) REFERENCES product(product_id) ON DELETE CASCADE
);


-- public.product_promo definition

-- Drop table

-- DROP TABLE product_promo;

CREATE TABLE product_promo (
	product_promo_id int8 NOT NULL,
	product_id int8 NOT NULL,
	promo_type varchar(255) NULL,
	status varchar(255) NOT NULL,
	expires_at timestamptz NOT NULL,
	created_at timestamptz NOT NULL,
	last_updated_at timestamptz NULL,
	CONSTRAINT product_promo_pk PRIMARY KEY (product_id),
	CONSTRAINT fk_product_promo_product_fk FOREIGN KEY (product_id) REFERENCES product(product_id)
);
CREATE INDEX product_promo_status_idx ON public.product_promo USING btree (status);


-- public.bundle_products definition

-- Drop table

-- DROP TABLE bundle_products;

CREATE TABLE bundle_products (
	product_bundle_id int8 NOT NULL,
	product_id int8 NOT NULL,
	CONSTRAINT bundle_products_unique UNIQUE (product_bundle_id, product_id),
	CONSTRAINT bundle_products_bundle_fk FOREIGN KEY (product_bundle_id) REFERENCES product(product_id),
	CONSTRAINT bundle_products_product_fk FOREIGN KEY (product_id) REFERENCES product(product_id)
);

-- public.product_tag definition

-- Drop table

-- DROP TABLE product_tag;

CREATE TABLE product_tag (
	product_id int8 NOT NULL,
	tag_id int8 NOT NULL,
	CONSTRAINT product_tag_pk PRIMARY KEY (product_id, tag_id)
);
CREATE INDEX product_tag_product_id_idx ON public.product_tag USING btree (product_id);
CREATE INDEX product_tag_tag_id_idx ON public.product_tag USING btree (tag_id);


-- public.tag definition

-- Drop table

-- DROP TABLE tag;

CREATE TABLE tag (
	tag_id int8 NOT NULL,
	slug text NOT NULL,
	in_landing_page bool DEFAULT false NOT NULL,
	category bool DEFAULT false NOT NULL,
	curated bool DEFAULT false NOT NULL,
	page_content text NULL,
	CONSTRAINT tag_pk PRIMARY KEY (tag_id)
);
CREATE INDEX tag_slug_idx ON public.tag USING btree (slug);


-- public.tag_relation definition

-- Drop table

-- DROP TABLE tag_relation;

CREATE TABLE tag_relation (
	tag_id int8 NOT NULL,
	related_tag_id int8 NOT NULL,
	CONSTRAINT tag_relation_pkey PRIMARY KEY (tag_id, related_tag_id),
	CONSTRAINT tag_relation_tag_fk FOREIGN KEY (tag_id) REFERENCES tag(tag_id),
	CONSTRAINT tag_relation_tag_fk_1 FOREIGN KEY (related_tag_id) REFERENCES tag(tag_id)
);
CREATE INDEX tag_relation_tag_id_idx ON public.tag_relation USING btree (tag_id);

-- public.product_popular_7_days source

CREATE MATERIALIZED VIEW product_popular_7_days
TABLESPACE pg_default
AS SELECT p.product_id,
    p.author_id,
    p.category_id,
    p.price_in_cents,
    p.title,
    p.slug,
    p.description,
    p.main_image,
    p.images,
    p.assets,
    p.product_type,
    p.product_status,
    p.metadata,
    p.created_at,
    p.last_updated_at,
    p.status,
    count(*) AS download_count
   FROM product_download pd
     LEFT JOIN product p ON pd.product_id = p.product_id
  WHERE pd.downloaded_at_day_normalized >= (floor(EXTRACT(epoch FROM now()) / 86400::numeric)::bigint - 7)
  GROUP BY p.product_id
WITH DATA;
