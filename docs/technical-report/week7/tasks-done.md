### 1. write examples for the 8 endpoints of rest-api


### 2. optimize postgresql tables for less use of disk
 
- 0. `batch_id` needs to be in the tables in teh primary key and we can not remove it, becuaase the upsert logic is based off of that 

- 1. Optimize aggregate table indexes
- 2. Storage optimization for aggregate tables