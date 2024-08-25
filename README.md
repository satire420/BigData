# BigData-AE | Team-77 | Coursework


## Overview

This project centers on the development of a scalable and performant text search and filtering system built upon the Apache Spark framework.  To handle large volumes of data, it intelligently integrates text preprocessing techniques (such as cleaning, normalization, and potentially stemming or lemmatization) to prepare the text corpus for analysis.  Relevance ranking is achieved through the implementation of the DPH scoring model, which likely considers factors such as term frequency, document length, and overall corpus statistics. Furthermore, to ensure the quality and uniqueness of results, the system employs deduplication strategies, potentially using similarity metrics or advanced hashing techniques to identify redundant documents.  Throughout the entire pipeline, the project leverages Apache Spark's distributed computing capabilities, including data partitioning, resilient in-memory computations, and optimized transformations/actions, to achieve scalability and efficiency in handling vast datasets and diverse user queries.


## Features
- **Text Preprocessing**: Cleansing and normalization for optimal processing.
- **DPH Scoring**: Advanced document ranking based on relevance.
- **Redundancy Filtering**: Eliminates duplicate results for clarity.


## Getting Started
- Install Apache Spark and Java 11+.
- Clone the repository.
- Compile the project (e.g., using Maven or SBT).
- Configure Spark settings for job submissions.
- Submit Spark jobs with necessary command-line arguments.
- Refer to troubleshooting for common errors.


## Usage
Package and submit the Spark job as detailed in the installation instructions. The engine processes input documents and queries, outputting ranked search results.


## Contribution
Contributions are welcome. Please refer to the project guidelines for submitting contributions.
