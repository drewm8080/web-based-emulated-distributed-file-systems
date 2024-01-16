# Project Overview

**Contributors**:Aditya Singh, Andrew Moore, Thanawan Lertmongkolnam, Vorapoom Thirapatarapong, Xinxin Xu

**Video Showcase**: : https://www.youtube.com/watch?v=EC6Q2U3nW7s

**Grade**: 100%

## Description
This project aims to study the factors affecting car accidents in the United States and predict the number of car accidents based on these factors. The project involves the use of three emulated distributed file systems (EDFS), including Firebase, MySQL, and MongoDB, to store the data. The datasets are explored, cleaned, and prepared for analysis.

## Data Storage and Metadata
The data storage methods and metadata storage methods for each EDFS are as follows:
- **Firebase**:
  - Data storage method: Folders and files are stored in JSON format.
  - Metadata storage method: Metadata is stored along with the file name.
  - Directory table: Stores the directory structure of the file system using parent_id and child_id columns.
  - Partition table: Stores partition IDs of each file.
- **MySQL**:
  - Data storage method: Utilizes relational tables to store structured data.
  - Metadata storage method: Metadata is stored in separate tables linked to the main data tables.
  - File Table (file_id, file_name): Stores the information of file or folder’s ID and name.
  - Partition Table (file_id, partition_id): Stores partition IDs of each file.

- **MongoDB**:
  - Data storage method: Stores data in a flexible, JSON-like format, allowing for dynamic schemas.
  - Metadata storage method: Utilizes collections to store metadata and simulate file and folder structure.
  - Directory table: Stores the directory structure of the file system using parent_id and child_id columns.
  - Partition table: Stores partition IDs of each file.

## Web Interface
A web application has been designed to showcase the project results. It allows users to interact with the databases through terminal commands, navigate through the databases, and perform search queries. The web interface consists of three tabs:
1. **Terminal commands and Navigation**: Modify the databases using terminal commands and display the structure of each database.
2. **Search function**: Search the databases using SQL queries.
3. **Analytics function**: Display outputs from regression analysis and allow prediction based on input attributes.

## Terminal Commands
The web interface supports 7 different terminal commands, each of which communicates with the corresponding database and returns the results in the output window accordingly.

## Interactive Navigation
Users can interactively navigate through the databases by specifying the database they want to explore. The database structure is displayed in JSON format and represented as a tree, allowing users to explore the elements.

## Search Function
The search function enables users to perform SQL queries on the uploaded datasets. Users can specify the database to search on, the method of map-reduce function, and the SQL query.

## Analytics Function
The web application includes a section explaining the regression analysis and allows users to predict the car accident rate by inputting several attributes based on trained models.

