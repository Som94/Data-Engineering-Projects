# Azure Data Lake Storage Design

Storage Account

stenergylake001

Container (Filesystem)

energy-data

Folder Structure

energy-data/

│

├── bronze/

│      ├── customer/

│      ├── meter/

│      ├── billing/

│      ├── weather/

│      ├── tariff/

│      └── region/

│

├── silver/

│      ├── customer/

│      ├── meter/

│      ├── billing/

│      ├── weather/

│      ├── tariff/

│      └── region/

│

├── gold/

│      ├── fact/

│      ├── dimension/

│      ├── reports/

│      └── powerbi/

│

├── metadata/

│      ├── watermark/

│      ├── audit/

│      ├── config/

│      ├── schema/

│      └── pipeline/

│

├── archive/

├── logs/

└── temp/