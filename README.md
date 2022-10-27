# Reusable Dataform Slowly Changing Dimension Type 2 Backfill for Changing schema and initial loads

**Slowly Changing Dimension**

A slowly changing dimension in data warehousing is a dimension which contains relatively static data which can change slowly but unpredictably, rather than according to a regular schedule.

**Slowly Changing Dimension Type 2**

Row versioning, Track changes as version records with current flag & active dates and other metadata.

**Use cases for this Code?**

1 - Initial Load
2 - History Backfill for schema changes
3 - Reload for a specific timestamp due to data ingestion issues/corrupt date, etc

**Variables**

Key variables in this code is the Source Table details (Catalog, schema and Table name), Target Table details (Catalog, schema and Table name), From date, To date and Portion Size. The other very important variable that we can set (just for CLI at the moment when this Readme was written) is the concurrency limits. This variable helps us set the number of parallel steps we want to execute at a time. The hash (unique and non_unique), eff_date and expiry date column names are optional, but if there are any specific naming conventions it could be used.

![image](https://user-images.githubusercontent.com/48508718/198388617-73e9ff7d-7bad-4889-9f95-4765fa5b826a.png)

**Reusable?**

The code divides the From and To Date into portions size you enter and merge everything together. So in case of the data being huge for each partion, we could process it for daily all parallel (in case we want to limit, the concurrency could be used to define the max parallel tasks). And in case of smaller partition size we could do all at once. 

![Untitled Diagram (1)](https://user-images.githubusercontent.com/48508718/198388574-10b125e4-3d94-44bd-b13f-eece77233a8c.png)


**Flowchart** 

![image](https://user-images.githubusercontent.com/48508718/198382310-4d6e8ede-6e8d-4003-81e2-236b6bb10c60.png)

The future steps to this code is a set of assertions to confirm if the data looks as expected. Also, depending on the use case we could either swap the final table created by this process or use a part of it as needed after renaming and droping the processing columns like hashing columns, min_source_eff_date, etc.
