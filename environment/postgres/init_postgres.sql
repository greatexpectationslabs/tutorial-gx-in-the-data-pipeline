-- Create role gx_user.
do $$ begin if not exists (
    select 1
    from pg_roles
    where rolname = 'gx_user'
) then create user gx_user with encrypted password 'gx_user_password';
end if;
end $$;

-- Create database gx and grant permissions to gx_user.
do $$ begin if not exists (
    select 1
    from pg_database
    where datname = 'gx'
) then create database gx;
grant all privileges on database gx to gx_user;
grant all on schema public to gx_user;
end if;
end $$;

-- Create tutorial table(s) in gx database.
create table public.customers (
    customer_id bigint primary key,
    name text,
    dob date,
    city text,
    state text,
    zip text,
    country varchar(2)
);

create table public.products (
    product_id bigint primary key,
    name text,
    brand text,
    color text,
    unit_cost_usd double precision,
    unit_price_usd double precision,
    product_category_id bigint,
    product_subcategory_id bigint
);

create table public.product_category (
    product_category_id bigint primary key,
    name text
);

create table public.product_subcategory (
    product_subcategory_id bigint primary key,
    name text
);

-- Create role airflow_user.
do $$ begin if not exists (
    select 1
    from pg_roles
    where rolname = 'airflow_user'
) then create user airflow_user with encrypted password 'airflow_user_password';
end if;
end $$;
alter user airflow_user with superuser;

-- Create database airflow and grant permissions to airflow_user.
create database airflow;
grant all privileges on database airflow to airflow_user;
grant all on schema public to airflow_user;
