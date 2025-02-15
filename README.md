# tutorial-gx-in-the-data-pipeline
This repo hosts hands-on tutorials that guide you through working examples of GX data validation in a data pipeline.

While Airflow is used as the data pipeline orchestrator for the tutorials, these examples are meant to show how GX can be integrated into any orchestrator that supports Python code.

If you are new to GX, these tutorials will introduce you to GX concepts and guide you through creating GX data validation workflows that can be triggered and run using a Python-enabled orchestrator.

If you are an experienced GX user, these tutorials will provide code examples of GX and orchestrator integration that can be used as a source of best practices and techniques that can enhance your current data validation pipeline implementations.

## README table of contents
1. [Prerequisites](#prerequisites)
1. [Quickstart](#quickstart)
1. [Cookbooks](#cookbooks)
1. [Tutorial environment](#tutorial-environment)
1. [Tutorial data](#tutorial-data)
1. [Troubleshooting](#troubleshooting)
1. [Additional resources](#additional-resources)

## Prerequisites
* Docker: You use Docker compose to run the containerized tutorial environment. [Docker Desktop](https://www.docker.com/products/docker-desktop/) is recommended.

* Git: You use Git to clone this repository and access the contents locally. Download Git [here](https://git-scm.com/downloads).

* GX Cloud [organization id and access token](https://docs.greatexpectations.io/docs/cloud/connect/connect_python#get-your-user-access-token-and-organization-id): Cookbook 3 uses GX Cloud to store and visualize data validation results. Sign up for a free GX Cloud account [here](https://hubs.ly/Q02TyCZS0).


## Quickstart
1. Clone this repo locally.
    ```
    git clone https://github.com/greatexpectationslabs/tutorial-gx-in-the-data-pipeline.git
    ```

2. Change directory into the repo root directory.
   ```
   cd tutorial-gx-in-the-data-pipeline
   ```

3. Start the tutorial environment using Docker compose. **If you are running Cookbook 3, supply your GX Cloud credentials.**

   * To run the environment for Cookbooks 1 or 2:
      ```
      docker compose up --build --detach --wait
      ```

   * To run the environment for Cookbooks 1, 2, or 3, replace `<my-gx-cloud-org-id>` and `<my-gx-cloud-access-token>` with your GX Cloud organization id and access token values, respectively:
      ```
      export GX_CLOUD_ORGANIZATION_ID="<my-gx-cloud-org-id>"
      export GX_CLOUD_ACCESS_TOKEN="<my-gx-cloud-access-token>"
      docker compose up --build --detach --wait
      ```

> [!IMPORTANT]
> The first time that you start the Docker compose instance, the underlying Docker images need to be built. This process can take several minutes.
>
> **When environment is ready, you will see the following output (or similar) in the terminal:**
>
>```
>✔ Network tutorial-gx-in-the-data-pipeline_gxnet         Created
>✔ Container tutorial-gx-in-the-data-pipeline-postgres    Healthy
>✔ Container tutorial-gx-in-the-data-pipeline-airflow     Healthy
>✔ Container tutorial-gx-in-the-data-pipeline-jupyterlab  Healthy
>```

4. Access the JupyterLab (to run tutorial cookbooks) and Airflow (to run data pipelines) applications using a web browser.
   * JupyterLab: `http://localhost:8888/lab`
     * No credentials are required.
     * [Cookbooks](#cookbooks) can be opened from the JupyterLab File Browser.
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

> [!IMPORTANT]
> **Start the tutorial environment with Docker compose before accessing the path to the running tutorial cookbook.** The *Path to running tutorial cookbook* cell provides a localhost link to the running notebook that works when you are actively running the tutorial Docker compose instance.
>
> If the tutorial environment is not running when you try to access the cookbook, you will receive a connection error.

| No. | Cookbook topic | Path to running tutorial cookbook | Path to static render of cookbook |
| :--: | :-- | :-- | :-- |
| 1 | Data validation during ingestion of data into database (happy path) | [Click to open and run Cookbook 1](http://localhost:8888/lab/tree/Cookbook_1_Validate_data_during_ingestion_happy_path.ipynb) | [View Cookbook 1 on GitHub](cookbooks/Cookbook_1_Validate_data_during_ingestion_happy_path.ipynb) |
| 2 | Data validation during ingestion of data into database (pipeline fail + then take action) | [Click to open and run Cookbook 2](http://localhost:8888/lab/tree/Cookbook_2_Validate_data_during_ingestion_take_action_on_failures.ipynb) | [View Cookbook 2 on GitHub](cookbooks/Cookbook_2_Validate_data_during_ingestion_take_action_on_failures.ipynb) |
| 3 | Data validation with GX Core and GX Cloud \* | [Click to open and run Cookbook 3](http://localhost:8888/lab/tree/Cookbook_3_Validate_data_with_GX_Core_and_Cloud.ipynb) | [View Cookbook 3 on GitHub](cookbooks/Cookbook_3_Validate_data_with_GX_Core_and_Cloud.ipynb) |

<sup>\* Cookbook execution requires GX Cloud organization credentials. Sign up for a free GX Cloud account [here](https://hubs.ly/Q02TyCZS0).</sup>

## Tutorial environment
Tutorials are hosted and executed within a containerized environment that is run using Docker compose. The Docker compose setup uses the following containerized services:

* **JupyterLab**. The JupyterLab container hosts the tutorial cookbooks and provides a Python runtime environment for related tutorial scripts.

* **Airflow**. Airflow is the orchestrator used to implement and run the tutorial data pipelines. The Airflow container uses Astronomer's [Astro Runtime](https://www.astronomer.io/docs/astro/runtime-image-architecture#image-types) image.

* **Postgres**. The containerized Postgres database hosts the sample data used by the tutorial cookbooks and pipelines.

Cookbook 3 features GX Cloud-based data validation workflow that connects to your GX Cloud organization.

## Tutorial data

The data used in the tutorials is based on the [Global Electronics Retailers
dataset](https://www.kaggle.com/datasets/bhavikjikadara/global-electronics-retailers/data), available on Kaggle at the time of repo creation.

This dataset is used under the Creative Commons Attribution 4.0 International License. Appropriate credit is given to Bhavik Jikadara. The dataset has been modified to suit the requirements of this project. For more information about this license, please visit the [Creative Commons Attribution 4.0 International License](https://creativecommons.org/licenses/by/4.0/).

## Troubleshooting

This section provides guidance on how to resolve potential errors and unexpected behavior when running the tutorial.

### Docker compose errors
If you receive unexpected errors when running `docker compose up`, or do not get healthy containers, you can try recreating the tutorial Docker containers using the `--force-recreate` argument.
```
docker compose up --build --force-recreate --detach --wait
```

### GX Cloud environment variables warning
The tutorial Docker compose `docker-compose.yaml` is defined to capture `GX_CLOUD_ORGANIZATION_ID` and `GX_CLOUD_ACCESS_TOKEN` environment variables to support Cookbook 3. If these variables are not provided when running `docker compose up`, you will see the following warnings:

```
WARN[0000] The "GX_CLOUD_ORGANIZATION_ID" variable is not set. Defaulting to a blank string.
WARN[0000] The "GX_CLOUD_ACCESS_TOKEN" variable is not set. Defaulting to a blank string.
WARN[0000] The "GX_CLOUD_ORGANIZATION_ID" variable is not set. Defaulting to a blank string.
WARN[0000] The "GX_CLOUD_ACCESS_TOKEN" variable is not set. Defaulting to a blank string.
```

You can safely ignore the these warnings if:
* You are not trying to run Cookbook 3.
* You are running `docker compose down --volumes` to stop the running Docker compose.

## Additional resources

* To report a bug for any of the tutorials or code within this repo, [open an issue](https://github.com/greatexpectationslabs/tutorial-gx-in-the-data-pipeline/issues/new).

* To learn more about using GX, explore the GX docs. The [GX Cloud overview](https://docs.greatexpectations.io/docs/cloud/overview/gx_cloud_overview) and [GX Core introduction](https://docs.greatexpectations.io/docs/core/introduction/) pages are a great place to start.

* To ask questions about using GX, reach out on the [GX Discourse forum](https://discourse.greatexpectations.io/).
