create schema if not exists partman;
create schema if not exists public;

CREATE
EXTENSION IF NOT EXISTS ltree;
CREATE
EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;

-- public.product_tag definition

-- Drop table

-- DROP TABLE product_tag;

CREATE TABLE product_tag
(
    product_id int8 NOT NULL,
    tag_id     int8 NOT NULL,
    product_created_at timestamptz NULL,
    CONSTRAINT product_tag_pk PRIMARY KEY (product_id, tag_id)
) PARTITION BY HASH (tag_id);

DO
$$
DECLARE
i INT;
    num_partitions
INT := 64; -- total number of partitions
BEGIN
FOR i IN 0..(num_partitions - 1) LOOP
        EXECUTE format(
            'CREATE TABLE product_tag_p%s PARTITION OF product_tag FOR VALUES WITH (MODULUS %s, REMAINDER %s);',
            i, num_partitions, i
        );
END LOOP;
END $$;


-- public.tag definition

-- Drop table

-- DROP TABLE tag;

CREATE TABLE tag
(
    tag_id          int8               NOT NULL,
    slug            text               NOT NULL,
    in_landing_page bool DEFAULT false NOT NULL,
    category        bool DEFAULT false NOT NULL,
    curated         bool DEFAULT false NOT NULL,
    page_content    text NULL,
    CONSTRAINT tag_pk PRIMARY KEY (tag_id)
);
CREATE INDEX tag_slug_idx ON public.tag USING btree (slug);


-- public.category definition

-- Drop table

-- DROP TABLE category;

CREATE TABLE category
(
    category_id              int8 NOT NULL,
    parent_category_id       int8 NULL,
    default_name             text NOT NULL,
    default_description      text NOT NULL,
    created_at               timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at               timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    "attributes"             jsonb NULL,
    url_path                 text NOT NULL,
    hierarchy_path public.ltree NULL,
    name_translations        jsonb NULL,
    description_translations jsonb NULL,
    migration_status_id      varchar(100) NULL,
    CONSTRAINT category_pk PRIMARY KEY (category_id),
    CONSTRAINT category_parent_category_id_fk FOREIGN KEY (parent_category_id) REFERENCES category (category_id) ON DELETE SET NULL
);


-- public.product definition

-- Drop table

-- DROP TABLE product;

CREATE TABLE product
(
    product_id      int8                                  NOT NULL,
    author_id       int8                                  NOT NULL,
    category_id     int8 NULL,
    price_in_cents  int8                                  NOT NULL,
    title           jsonb                                 NOT NULL,
    slug            jsonb                                 NOT NULL,
    description     jsonb                                 NOT NULL,
    main_image      jsonb NULL,
    images          jsonb NULL,
    assets          jsonb                                 NOT NULL,
    product_type    text                                  NOT NULL,
    product_status  text                                  NOT NULL,
    metadata        jsonb                                 NOT NULL,
    created_at      timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
    last_updated_at timestamptz NULL,
    status          varchar NULL,
    CONSTRAINT product_pk PRIMARY KEY (product_id, category_id),
    CONSTRAINT product_category_id_fk FOREIGN KEY (category_id) REFERENCES category (category_id) ON DELETE SET NULL
) partition by list (category_id);

-- public.product_download definition

-- Drop table

-- DROP TABLE product_download;

CREATE TABLE product_download
(
    download_id                  int8        NOT NULL,
    product_id                   int8        NOT NULL,
    downloaded_at                timestamptz NOT NULL,
    downloaded_at_day_normalized int8 NULL,
    CONSTRAINT product_download_pk UNIQUE (download_id)
);

-- public.product_product_category definition

-- Drop table

-- DROP TABLE product_product_category;

CREATE TABLE product_product_category
(
    product_id  int8 NOT NULL,
    category_id int8 NOT NULL,
    CONSTRAINT product_product_category_pk PRIMARY KEY (product_id, category_id),
    CONSTRAINT product_product_category_category_id_fk FOREIGN KEY (category_id) REFERENCES category (category_id) ON DELETE CASCADE
);

-- public.product_promo definition

-- Drop table

-- DROP TABLE product_promo;

CREATE TABLE product_promo
(
    product_promo_id int8         NOT NULL,
    product_id       int8         NOT NULL,
    promo_type       varchar(255) NULL,
    status           varchar(255) NOT NULL,
    expires_at       timestamptz  NOT NULL,
    created_at       timestamptz  NOT NULL,
    last_updated_at  timestamptz NULL,
    CONSTRAINT product_promo_pk PRIMARY KEY (product_id)
);
CREATE INDEX product_promo_status_idx ON public.product_promo USING btree (status);


-- public.tag_relation definition

-- Drop table

-- DROP TABLE tag_relation;

CREATE TABLE tag_relation
(
    tag_id         int8 NOT NULL,
    related_tag_id int8 NOT NULL,
    CONSTRAINT tag_relation_pkey PRIMARY KEY (tag_id, related_tag_id),
    CONSTRAINT tag_relation_tag_fk FOREIGN KEY (tag_id) REFERENCES tag (tag_id),
    CONSTRAINT tag_relation_tag_fk_1 FOREIGN KEY (related_tag_id) REFERENCES tag (tag_id)
);
CREATE INDEX tag_relation_tag_id_idx ON public.tag_relation USING btree (tag_id);


-- public.bundle_products definition

-- Drop table

-- DROP TABLE bundle_products;

CREATE TABLE bundle_products
(
    product_bundle_id int8 NOT NULL,
    product_id        int8 NOT NULL,
    CONSTRAINT bundle_products_unique UNIQUE (product_bundle_id, product_id)
);

CREATE TABLE partman.product_template
(
    LIKE public.product INCLUDING ALL
);

-- Add constraints or indexes to the template table
ALTER TABLE partman.product_template
    ADD CONSTRAINT product_category_id_unique UNIQUE (category_id);

SELECT partman.create_parent(
               p_parent_table := 'public.product'::text,
               p_control := 'category_id'::text,
               p_type := 'list'::text,
               p_interval := '-1', -- placeholder for LIST partition
               p_template_table := 'partman.product_template'::text
       );

CREATE TABLE public.product_graphics PARTITION OF public.product
    FOR VALUES IN
(
    553
);
CREATE TABLE public.product_fonts PARTITION OF public.product
    FOR VALUES IN
(
    23
);
CREATE TABLE public.product_crafts PARTITION OF public.product
    FOR VALUES IN
(
    26
);
CREATE TABLE public.product_embroidery PARTITION OF public.product
    FOR VALUES IN
(
    735
);
CREATE TABLE public.product_laser_cutting PARTITION OF public.product
    FOR VALUES IN
(
    2245
);
CREATE TABLE public.product_bundles PARTITION OF public.product
    FOR VALUES IN
(
    546
);
CREATE TABLE public.product_3d_svg PARTITION OF public.product
    FOR VALUES IN
(
    1850
);
CREATE TABLE public.product_3d_printing PARTITION OF public.product
    FOR VALUES IN
(
    2244
);
CREATE TABLE public.product_knitting PARTITION OF public.product
    FOR VALUES IN
(
    2246
);

-- Create the heavies indexed after the inserts
-- Product
CREATE index if not exists product_author_id_idx ON public.product USING btree (author_id);
CREATE index if not exists product_category_id_idx ON public.product USING btree (category_id);
CREATE INDEX if not exists product_category_id_status_created_at_desc_idx ON public.product USING btree (category_id, status, created_at DESC);
CREATE INDEX if not exists product_category_id_status_desc_idx ON public.product USING btree (category_id, status DESC);
CREATE INDEX if not exists product_status_created_at_desc_idx ON public.product USING btree (status, created_at DESC);
CREATE INDEX if not exists product_status_idx ON public.product USING btree (status);
CREATE INDEX if not exists product_prodct_id_status_idx ON public.product USING btree (product_id, status);
-- Product category
CREATE INDEX if not exists product_product_category_category_id_idx ON public.product_product_category USING btree (category_id);
CREATE INDEX if not exists product_product_category_product_id_idx ON public.product_product_category USING btree (product_id);
CREATE INDEX if not exists product_product_category_category_id_product_id_idx ON public.product_product_category USING btree (category_id, product_id);
-- Product tag
CREATE INDEX if not exists product_tag_product_id_idx ON public.product_tag USING btree (product_id);
CREATE INDEX if not exists product_tag_tag_id_idx ON public.product_tag USING btree (tag_id);
CREATE INDEX product_tag_product_created_at_idx ON ONLY public.product_tag USING btree (product_created_at DESC);
-- Product download
CREATE index if not exists product_download_downloaded_at_day_normalized_idx ON public.product_download USING btree (downloaded_at_day_normalized);
CREATE index if not exists product_download_downloaded_at_idx ON public.product_download USING brin (downloaded_at);
CREATE index if not exists product_download_product_idt_idx ON public.product_download USING btree (product_id);

CREATE
MATERIALIZED VIEW mv_product_downloads_last_7d AS
SELECT p.category_id, p.product_id, COALESCE(rd.download_count, 0::bigint) AS download_count
FROM product p
         LEFT JOIN (SELECT pd.product_id,
                           count(*) AS download_count
                    FROM product_download pd
                    WHERE pd.downloaded_at_day_normalized >=
                          (floor(EXTRACT(epoch FROM now()) / 86400::numeric)::bigint - 7)
                    GROUP BY pd.product_id) rd ON p.product_id = rd.product_id
WHERE NOT (EXISTS (SELECT 1
                   FROM product_promo pp
                   WHERE pp.product_id = p.product_id));
CREATE INDEX mv_product_downloads_last_7d_download_count_idx ON public.mv_product_downloads_last_7d USING btree (download_count DESC);
CREATE INDEX mv_product_downloads_last_7d_product_id_download_count_desc_idx ON public.mv_product_downloads_last_7d USING btree (product_id, download_count DESC);
CREATE INDEX mv_product_downloads_last_7d_product_id_idx ON public.mv_product_downloads_last_7d USING btree (product_id);
