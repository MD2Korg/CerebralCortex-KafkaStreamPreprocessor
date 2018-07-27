# CerebralCortex-KafkaStreamPreprocessor

Cerebral Cortex is the big data cloud companion of mCerebrum designed to support population-scale data analysis, visualization, model development, and intervention design for mobile sensor data.

CerebralCortex-KafkaStreamPreprocessor (CC-KSP) is a apache-spark based pub/sub system for processing incoming mobile sensor data.

You can find more information about MD2K software on our [software website](https://md2k.org/software) or the MD2K organization on our [MD2K website](https://md2k.org/).

## How it works?
* CerebralCortex-APIServer uploads and publish publish file names on Kafka queue.
* CC-KSP process consumes messages published by [CerebralCortex-APIServer](https://github.com/MD2Korg/CerebralCortex-APIServer)  and:
    * Processes .gz files uploaded using Processed data
    * Converts CSV/JSON data into CerebralCortex stream format
    * Stores processed raw data and metadata in Cassandra and MySQL using CerebralCortex [DataStorageEngine](https://github.com/MD2Korg/CerebralCortex/tree/master/cerebralcortex/kernel/DataStoreEngine)
    * Stores processed data in InfluxDB for visualization

## Install

### Setup environment
To run CC-KSP, Clone/install and configure:
* [Python3.5](https://www.python.org/downloads/release/python-350/)
* [CerebralCortex-DockerCompose](https://github.com/MD2Korg/CerebralCortex-DockerCompose)
* [Apache Spark 2.2.0](https://spark.apache.org/releases/spark-release-2-2-0.html) 
* [CerebralCortex](https://github.com/MD2Korg/CerebralCortex-2.0.git)

### Clone and configure CC-KSP
* `git clone https://github.com/MD2Korg/CerebralCortex-KafkaStreamPreprocessor.git`
* Configure database url, username, and passwords in [cerebralcortex_apiserver.yml](https://github.com/MD2Korg/CerebralCortex-KafkaStreamPreprocessor/blob/master/cerebralcortex_apiserver.yml)
    * Please do not change other options in configurations unless you have changed them in CerebralCortex-DockerCompose   
* Install dependencies:
    * `pip install -r requirements.txt`
* Run following command to start CC-KSP:
    * `sh run.sh` (Please update the paths in run.sh, read comments)

## Contributing
Please read our [Contributing Guidelines](https://md2k.org/contributing/contributing-guidelines.html) for details on the process for submitting pull requests to us.

We use the [Python PEP 8 Style Guide](https://www.python.org/dev/peps/pep-0008/).

Our [Code of Conduct](https://md2k.org/contributing/code-of-conduct.html) is the [Contributor Covenant](https://www.contributor-covenant.org/).

Bug reports can be submitted through [JIRA](https://md2korg.atlassian.net/secure/Dashboard.jspa).

Our discussion forum can be found [here](https://discuss.md2k.org/).

## Versioning

We use [Semantic Versioning](https://semver.org/) for versioning the software which is based on the following guidelines.

MAJOR.MINOR.PATCH (example: 3.0.12)

  1. MAJOR version when incompatible API changes are made,
  2. MINOR version when functionality is added in a backwards-compatible manner, and
  3. PATCH version when backwards-compatible bug fixes are introduced.

For the versions available, see [this repository's tags](https://github.com/MD2Korg/CerebralCortex-KafkaStreamPreprocessor/tags).

## Contributors

Link to the [list of contributors](https://github.com/MD2Korg/CerebralCortex-KafkaStreamPreprocessor/graphs/contributors) who participated in this project.

## License

This project is licensed under the BSD 2-Clause - see the [license](https://md2k.org/software-under-the-hood/software-uth-license) file for details.

## Acknowledgments

* [National Institutes of Health](https://www.nih.gov/) - [Big Data to Knowledge Initiative](https://datascience.nih.gov/bd2k)
  * Grants: R01MD010362, 1UG1DA04030901, 1U54EB020404, 1R01CA190329, 1R01DE02524, R00MD010468, 3UH2DA041713, 10555SC
* [National Science Foundation](https://www.nsf.gov/)
  * Grants: 1640813, 1722646
* [Intelligence Advanced Research Projects Activity](https://www.iarpa.gov/)
  * Contract: 2017-17042800006

