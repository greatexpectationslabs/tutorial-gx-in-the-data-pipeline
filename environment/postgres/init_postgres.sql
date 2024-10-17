-- Create role gx_user.
DO $$ BEGIN IF NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = 'gx_user'
) THEN CREATE USER gx_user WITH ENCRYPTED PASSWORD 'gx_user_password';
END IF;
END $$;

-- Create database gx and grant permissions to gx_user.
DO $$ BEGIN IF NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = 'gx'
) THEN CREATE DATABASE gx;
GRANT ALL PRIVILEGES ON DATABASE gx TO gx_user;
GRANT ALL ON SCHEMA public TO gx_user;
END IF;
END $$;

-- Create tutorial table(s) in gx database.
CREATE TABLE public.customers (
    customer_id bigint primary key,
    name text,
    dob date,
    city text,
    state text,
    zip text,
    country varchar(2)
);

-- Create role airflow_user.
DO $$ BEGIN IF NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = 'airflow_user'
) THEN CREATE USER airflow_user WITH ENCRYPTED PASSWORD 'airflow_user_password';
END IF;
END $$;
ALTER USER airflow_user WITH SUPERUSER;

-- Create database airflow and grant permissions to airflow_user.
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow_user;
GRANT ALL ON SCHEMA public TO airflow_user;
