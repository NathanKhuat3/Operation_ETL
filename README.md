# Operation_ETL

![image](https://github.com/user-attachments/assets/8739184e-19a4-431b-bb8c-9a3a7f7554a4)

### Situation

Bank XYZ started implementing a new software to support 3 different types of transactions in April. In June, the manager and the software team wants to know about trends in processing time and cost for each transaction type since the software was introduced.

### Business Ask

Create an automated ETL pipeline that extract the data from a Data Lake, clean the data and transform the data Schema, load the data into Redshift and Athena for different use cases. Investigate the Transaction types that are most time consuming and costly. How we can optimize these transaction types?

### Pipeline's Structure
![image](https://github.com/user-attachments/assets/1b3a55d1-d97b-4163-a74c-399d5a2e1a30)

### Overview

This automated data pipeline runs on a monthly basis. It first extracts dirty data from the dirty-transaction S3 bucket and runs a Python script which cleans the data and upload the cleaned data to the cleaned-transaction S3 bucket. Next, AWS crawlers crawl the data from the cleaned-transaction S3 bucket and store the metadata in the AWS Glue Data Catalog, which can then be utilized by Amazon Athena and Amazon Redshift for analysis purposes. A PowerBI dashboard is then created from the Amazon Redshift Database.

### Tools:
* AWS EC2
* AWS S3
* AWS Redshift
* AWS Athena
* AWS Glue
* Apache Airflow
* Power BI

### Metrics
* Cost: The cost incurred by the transaction
* Duration: The time it takes for the transaction to be processed
* Errors: The number of software errors reported by employees as each transaction is being processed

### Dimensions
* Customer Segment: The type of customer is either Individual or Corporate
* Transaction Type: The 3 types of transactions are: Withdrawl, Loan and Deposit

### Summary  of insights
* Loan Transactions have the highest average cost and longest processing time while also having the highest deviation across transactions, demonstrating the most inconsistent performance in both cost and processing time
* In contrast, Withdrawal transactions have the lowest average cost, processing time and standard deviation between accounts. The software shows consistent performance for this type of transaction.
* No significant difference between the Individual and Corporate Client Segment in regards to Processing Time, Cost and Number of Errors
* Around 26% of the transactions still encountered technical software issues

### Recommendations and Next Steps
* Conduct cost-benefit analysis based on the data to see if the software implementation should be continued
* Investigate characteristics of Loan Transactions to implement new features that enhance the software's ability to process loans
* Examine accounts with technical errors to examine common sources of technical issues and upgrading the software, with a target to achieve 10% error rate in production
* Investigate if the drop off in the number of transactions over time for Loan Transactions and in May for Withdrawal and Deposit Transactions are due to demand or operation bottlenecks due to software

![image](https://github.com/user-attachments/assets/1096f950-bf73-48e7-8275-8b3e20714b20)
