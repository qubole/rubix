One time setup
==============
1. Add your QDS_AUTH_TOKEN in rubix_stressor.py
2. Add fact tables' DDLs in fact_tables.sql
3. Add/Update dimension tables' DDLs in dimensions.sql

How to run
==========
(Works on api.qubole right now)
1. Run fact_tables.sql via Hive in your account
2. Run dimensions.sql via Hive in your account
3. Start a Presto cluster labelled "rubix-stress" with aggressive downscaling and size appropriate to test disk space based evictions
4. Change API_TOKEN as needed
5. Run `python rubix_stressor.py 2>&1 | tee logs` using python3
6. Ctrl-C to exit gracefully when done
7. Grep "failed" in `logs`
