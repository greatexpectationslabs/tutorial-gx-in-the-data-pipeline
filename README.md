# tutorial-gx-in-the-data-pipeline
This repo hosts hands-on tutorials that guide you through working examples of GX data validation in an Airflow pipeline.

If you are new to GX, these tutorials will introduce you to GX concepts and guide you through creating GX data validation workflows that can be triggered and run using Airflow.

If you are an experienced GX user, these tutorials will provide code examples of GX and Airflow integration that can be used as a source of best practices and techniques that can enhance your current data validation pipeline implementations.

## README table of contents
1. [Prerequisites](#prerequisites)
1. [Quickstart](#quickstart)
1. [Cookbooks](#cookbooks)
1. [Tutorial environment](#tutorial-environment)
1. [Tutorial data](#tutorial-data)
1. [Additional resources](#additional-resources)

## Prerequisites
* Docker: You use Docker compose to run the containerized tutorial environment. [Docker Desktop](https://www.docker.com/products/docker-desktop/) is recommended.

* Git: You use Git to clone this repository and access the contents locally. Download Git [here](https://git-scm.com/downloads).

## Quickstart
1. Clone this repo locally.
    ```
    git clone https://github.com/greatexpectationslabs/tutorial-gx-in-the-data-pipeline.git
    ```

2. Change directory into the repo root directory.
   ```
   cd tutorial-gx-in-the-data-pipeline
   ```

3. Start the tutorial environment using Docker compose.
   ```
   docker compose up --build --detach --wait
   ```

> [!NOTE]
> The first time that you start the Docker compose instance, the underlying Docker images need to be built. This process can take several minutes.

4. Access the JupyterLab (to run tutorial cookbooks) and Airflow (to run data pipelines) applications using a web browser.
   * JupyterLab: `http://localhost:8888/lab`
     * No credentials are required.
     * Cookbooks can be opened from the JupyterLab File Browser.
   * Airflow: `http://localhost:8080`
     * User: `admin`
     * Password: `gx`

5. Once you are finished running the tutorial environment, stop it using Docker compose.
   ```
   docker compose down --volumes
   ```

## Cookbooks

Tutorials are presented as "cookbooks" (JupyterLab notebooks) that can be run interactively and that provide written explanations of concepts alongside executable code.

Cookbooks will be progressively added to this repo; the table below lists the current and planned cookbook topics.

| Cookbook # | Cookbook topic | Cookbook status |
| :--: | :-- | :-- |
| 1 | Data validation during ingestion of data into database (happy path) | [Link to Cookbook 1](cookbooks/Cookbook_1_Validate_data_during_ingestion_happy_path.ipynb) |
| 2 | Data validation during ingestion of data into database (fail + then take action) | Coming soon |
| 3 | Data validation of Postgres database tables \* | Coming soon |
 | 4 | Data validation and automated handling in a medallion data pipeline \* | Coming soon |

<sup>\* Cookbook execution requires GX Cloud organization credentials. Sign up for a free GX Cloud account [here](https://hubs.ly/Q02TyCZS0).</sup>

## Tutorial environment
Tutorials are hosted and executed within a containerized environment that is run using Docker compose. The Docker compose setup uses the following containerized services:

* **JupyterLab**. The JupyterLab container hosts the tutorial cookbooks and provides a Python runtime environment for related tutorial scripts.

* **Airflow**. Airflow is the orchestrator used to implement and run the tutorial data pipelines. The Airflow container uses Astronomer's [Astro Runtime](https://www.astronomer.io/docs/astro/runtime-image-architecture#image-types) image.

* **Postgres**. The containerized Postgres database hosts the sample data used by the tutorial cookbooks and pipelines.

Cookbooks that feature GX Cloud-based data validation workflows connect to your GX Cloud organization.

## Tutorial data

The data used in the tutorials is based on the [Global Electronics Retailers
dataset](https://www.kaggle.com/datasets/bhavikjikadara/global-electronics-retailers/data), available on Kaggle at the time of repo creation.

This dataset is used under the Creative Commons Attribution 4.0 International License. Appropriate credit is given to Bhavik Jikadara. The dataset has been modified to suit the requirements of this project. For more information about this license, please visit the [Creative Commons Attribution 4.0 International License](https://creativecommons.org/licenses/by/4.0/).

## Additional resources

* To report a bug for any of the tutorials or code within this repo, [open an issue](https://github.com/greatexpectationslabs/tutorial-gx-in-the-data-pipeline/issues/new).

* To learn more about using GX, explore the GX docs. The [GX Cloud overview](https://docs.greatexpectations.io/docs/cloud/overview/gx_cloud_overview) and [GX Core introduction](https://docs.greatexpectations.io/docs/core/introduction/) pages are a great place to start.

* To ask questions about using GX, reach out on the [GX Discourse forum](https://discourse.greatexpectations.io/).
