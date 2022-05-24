-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

SELECT COUNT(*) FROM clinicaltrial_2021

-- COMMAND ----------

SELECT Type, COUNT(Type) FROM clinicaltrial_2021 group by Type order by count(Type) Desc

-- COMMAND ----------

select phone,count(phone) from clinicaltrial_2021 lateral view explode(split(conditions,',')) conditions as phone group by phone order by count(phone) desc limit 5

-- COMMAND ----------

create temp view view1 as
SELECT phone
FROM clinicaltrial_2021 lateral VIEW explode(split(conditions,',')) conditions AS phone

-- COMMAND ----------

select * from view1

-- COMMAND ----------

create temp view view2 as
select view1.phone,mesh.tree
from view1
inner join mesh on view1.phone=mesh.term 

-- COMMAND ----------

select * from view2

-- COMMAND ----------

SELECT substring(tree,1,3),count(substring(tree,1,3)) FROM view2 group by substring(tree,1,3) order by count(substring(tree,1,3)) desc 

-- COMMAND ----------

#task5

-- COMMAND ----------

create temp view pharmacy1 as
SELECT sponsor FROM clinicaltrial_2021 left join pharma on clinicaltrial_2021.sponsor=pharma.parent_company where pharma.parent_company is null

-- COMMAND ----------

SELECT sponsor,count(sponsor) FROM pharmacy1 group by sponsor order by count(sponsor) desc

-- COMMAND ----------

#task6

-- COMMAND ----------

select substring(completion,1,3),count(substring(completion,1,3)) from clinicaltrial_2021 where status="Completed" and completion like '% 2021' group by substring(completion,1,3) order by count(substring(completion,1,3)) desc

-- COMMAND ----------


