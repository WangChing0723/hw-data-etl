# Assignment - API Enhancement

## Output Screenshot
![Screenshot](output_screenshot.jpg)

## Description

Please see the target section down below and try to achieve the items as many as you can.

## Target

1. Use the following data set (__./data/cur.zip__) to analyze and complete ETL work

      | Column | Description |
      | -- | -- |
      | bill/PayerAccountId | The account ID of the paying account. For an organization in AWS Organizations, this is the account ID of the master account. |
      |lineItem/UnblendedCost | The UnblendedCost is the UnblendedRate multiplied by the UsageAmount. |
      | lineItem/UnblendedRate | The uncombined rate for specific usage. For line items that have an RI discount applied to them, the UnblendedRate is zero. Line items with an RI discount have a UsageType of Discounted Usage. |
      | lineItem/UsageAccountId |The ID of the account that used this line item. For organizations, this can be either the master account or a member account. You can use this field to track costs or usage by account. |
      | lineItem/UsageAmount | The amount of usage that you incurred during the specified time period. For size-flexible reserved instances, use the reservation/TotalReservedUnits column instead. |
      | lineItem/UsageType | The simple description about the service usage type |      
      | product/ProductName | The full name of the AWS service. Use this column to filter AWS usage by AWS service. Sample values: AWS Backup, AWS Config, Amazon Registrar, Amazon Elastic File System, Amazon Elastic Compute Cloud |
      | lineItem/LineItemType | The item's type: It would be Usage, Fee, Credit... |
      | lineItem/LineItemDescription | The item's detail description: It would coverd this item's type, price or some detail description |
1. Using the __fix.json__ to change the data to update the data sets.
    1. Only the part with __product/ProductName=="Amazon CloudFront"__ and __lineItem/LineItemType=="Usage"__ will be affected.
    2. __fix.json__ would contain the price need to be replace the column __lineItem/UnblendedRate__
        ```json
        ...
        {
            "lineItem/UsageAccountId": "467262080079",
            "lineItem/UsageType": "EU-DataTransfer-Out-Bytes",
            "lineItem/UnblendedRate": 0.05
        },
        ...
        ```
        The data before change:
        | lineItem/UsageAccountId | product/ProductName | lineItem/LineItemType | lineItem/UsageType | lineItem/LineItemDescription | lineItem/UsageAmount | lineItem/UnblendedRate | lineItem/UnblendedCost |
        | -- | -- | -- | -- | -- | -- | -- | -- |
        | 467262080079 | Amazon CloudFront | Usage | EU-DataTransfer-Out-Bytes | $0.003 per GB data transfer out (Europe) | 300 | 0.003000 | 0.9 |
        The data after change:
        | lineItem/UsageAccountId | product/ProductName | lineItem/LineItemType | lineItem/UsageType | lineItem/LineItemDescription | lineItem/UsageAmount | lineItem/UnblendedRate | lineItem/UnblendedCost |
        | -- | -- | -- | -- | -- | -- | -- | -- |
        | 467262080079 | Amazon CloudFront | Usage | EU-DataTransfer-Out-Bytes | $0.05 per GB data transfer out (Europe) | 300 | 0.05 | 1.5 |
    1. Notice: 
        1. __lineItem/UnblendedCost__ should be __lineItem/UsageAmount__ * __lineItem/UnblendedRate__
        2. if Rate shows in __lineItem/LineItemDescription__ must change the description.
1. Process this data set into multiple files and split by __lineItem/UsageAccountId__, the naming format is like {__lineItem/UsageAccountId__}.zip, and output destination would be ./output/
    - example
    ```bash
    data/
    - cur.zip
    - fix.json
    output/
    - 123456789012.zip
    - 123456789013.zip
    - 123456789014.zip
    ...
    ```
1. Your can only use __Python__ or __C#__ to implement.

## Deliverable

1. Upload codes to your __GitHub__ and __provide repo URL__.
1. Include a __README.md__ file in __the root of repo__.
1. Provide the steps of running your program.

## Notice

* Once you finished the assignment, send an email back to the one who contacted you.
* Leave comments to __README.md__ if there is any.
