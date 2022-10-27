var getDaysArray = function(start, end, portion) {
    end_dt=new Date(start);
      for(var arr=[],dt=new Date(start); dt<new Date(end); dt.setDate(dt.getDate()+portion)){
          end_dt.setDate(dt.getDate()+portion);
          if(end_dt>=new Date(end)){
            end_dt=new Date(end);
          }
          arr.push(((new Date(dt)).toISOString().slice(0,10)).concat(":",end_dt.toISOString().slice(0,10)));
  
      }
      return arr;
  };
  
  constructor() 
  {
      this.count = 0;
  }
  
  var daylist = getDaysArray(new Date(dataform.projectConfig.vars.from_date),new Date(dataform.projectConfig.vars.to_date),new Number(dataform.projectConfig.vars.portion_size));
  // daylist.map((v)=>v).join("")
  daylist.map(pair => pair.split(":")).forEach(daylist => {
  operate("scd2_" + this.count,{
      hasOutput: true
    }).queries(`DECLARE src_unique STRING;
  DECLARE src_non_unique STRING;
  DECLARE final_target_unique STRING;
  DECLARE src_non_unique_proc STRING;
  
  SET src_unique = (
  WITH selected_columns as (
  SELECT primary_key_column_name  
      FROM ${dataform.projectConfig.defaultSchema}.table_to_unique
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}"  and table_catalog = "${dataform.projectConfig.vars.src_database}" and table_schema = "${dataform.projectConfig.vars.src_schema}"
  )
  SELECT STRING_AGG(primary_key_column_name) AS columns FROM selected_columns
  );
  
  SET final_target_unique = (
  WITH selected_columns as (
  SELECT primary_key_column_name  
      FROM ${dataform.projectConfig.defaultSchema}.table_to_unique
      WHERE 
      table_name =  "covid_scd2_final"  and table_catalog = "${dataform.projectConfig.vars.src_database}" and table_schema = "${dataform.projectConfig.vars.src_schema}"
  )
  SELECT STRING_AGG(primary_key_column_name) AS columns FROM selected_columns
  );
  
  SET src_non_unique = (
  WITH selected_columns as (
  SELECT column_name 
      FROM ${dataform.projectConfig.vars.src_database}.${dataform.projectConfig.vars.src_schema}.INFORMATION_SCHEMA.COLUMNS
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}" and column_name not in (SELECT primary_key_column_name  
      FROM ${dataform.projectConfig.defaultSchema}.table_to_unique
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}"  and table_catalog = "${dataform.projectConfig.vars.src_database}" and table_schema = "${dataform.projectConfig.vars.src_schema}"
  ) and column_name <> "${dataform.projectConfig.vars.timestampfield}" 
  )
  SELECT STRING_AGG(column_name) AS columns FROM selected_columns
  );
  
  SET src_non_unique_proc = (
  WITH selected_columns as (
  SELECT concat(' coalesce(cast(',column_name,' as STRING),"NA")') as clean_data
      FROM ${dataform.projectConfig.vars.src_database}.${dataform.projectConfig.vars.src_schema}.INFORMATION_SCHEMA.COLUMNS
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}" and column_name not in (SELECT primary_key_column_name  
      FROM ${dataform.projectConfig.defaultSchema}.table_to_unique
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}"  and table_catalog = "${dataform.projectConfig.vars.src_database}" and table_schema = "${dataform.projectConfig.vars.src_schema}"
  ) and column_name <> "${dataform.projectConfig.vars.timestampfield}" 
  )
  SELECT STRING_AGG(clean_data) AS columns FROM selected_columns
  );
  
  
  EXECUTE IMMEDIATE format(
    """
  CREATE OR REPLACE TABLE ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_scd2_${this.count}
  as (Select %s,
    %s, ${dataform.projectConfig.vars.timestampfield}, ${dataform.projectConfig.vars.start_from_column_name}, ${dataform.projectConfig.vars.end_at_column_name},${dataform.projectConfig.vars.target_hash_unique_col_name},  ${dataform.projectConfig.vars.target_hash_non_unique_col_name}
      from (select
              %s, %s, ${dataform.projectConfig.vars.timestampfield},
              row_number() OVER (PARTITION BY %s, %s, grp order by ${dataform.projectConfig.vars.timestampfield} asc) as row_num, 
              grp,
              min(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s, %s,grp)  as ${dataform.projectConfig.vars.start_from_column_name},
              CASE 
                WHEN grp < 0 
                  THEN ${dataform.projectConfig.vars.timestampfield} 
                WHEN max(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s) = (SELECT max(${dataform.projectConfig.vars.timestampfield}) from ${dataform.projectConfig.vars.src_schema}.${dataform.projectConfig.vars.src_table} where ${dataform.projectConfig.vars.timestampfield} >= cast(substr('${daylist}',0,10) as Date) and ${dataform.projectConfig.vars.timestampfield} < cast(substr('${daylist}',12) as Date)) or ${dataform.projectConfig.vars.timestampfield} <> max(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s) 
                  THEN date_add((lead(${dataform.projectConfig.vars.timestampfield}) over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc)), INTERVAL -1 Day) 
                ELSE max(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s) 
                END as ${dataform.projectConfig.vars.end_at_column_name}, 
                ${dataform.projectConfig.vars.target_hash_unique_col_name},  ${dataform.projectConfig.vars.target_hash_non_unique_col_name}
            from ( 
                    select %s, %s, ${dataform.projectConfig.vars.timestampfield}, 
                    date_diff(lead(${dataform.projectConfig.vars.timestampfield}) over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc), ${dataform.projectConfig.vars.timestampfield} ,DAY),
                    CASE WHEN date_diff(lead(${dataform.projectConfig.vars.timestampfield}) over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc),${dataform.projectConfig.vars.timestampfield},DAY)>1 
                      THEN (row_number() over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc) - row_number() over (PARTITION BY %s, %s order by ${dataform.projectConfig.vars.timestampfield} asc))* (-1) 
                      ELSE row_number() over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc) -row_number() over (PARTITION BY %s,%sorder by ${dataform.projectConfig.vars.timestampfield} asc)
                      END as grp,
                      TO_BASE64(md5(IFNULL(NULLIF(UPPER(TRIM(CAST(CONCAT(%s)AS STRING))),''),'^^'))) as ${dataform.projectConfig.vars.target_hash_unique_col_name}, 
                      TO_BASE64(md5(IFNULL(NULLIF(UPPER(TRIM(CAST(CONCAT(%s)AS STRING))),''),'^^'))) as ${dataform.projectConfig.vars.target_hash_non_unique_col_name}
                    from (
                          SELECT DISTINCT * from ${dataform.projectConfig.vars.src_schema}.${dataform.projectConfig.vars.src_table} where            ${dataform.projectConfig.vars.timestampfield} >= cast(substr('${daylist}',0,10) as Date)
               and date < cast(substr('${daylist}',12) as Date) 
                          ) t
                    where ${dataform.projectConfig.vars.timestampfield} >= cast(substr('${daylist}',0,10) as Date) and ${dataform.projectConfig.vars.timestampfield} < cast(substr('${daylist}',12) as Date)
                  ) 
              ) as a 
      where row_num = (
        select max(row_num) from (
          select %s, %s, ${dataform.projectConfig.vars.timestampfield}, row_number() OVER (PARTITION BY %s, %s ,grp order by ${dataform.projectConfig.vars.timestampfield} asc) as row_num,
          grp,
          min(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s,%s,grp) as ${dataform.projectConfig.vars.start_from_column_name},
          -- lead(${dataform.projectConfig.vars.timestampfield}) over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc) as ${dataform.projectConfig.vars.end_at_column_name},
          CASE 
            WHEN max(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s) = (
              SELECT max(${dataform.projectConfig.vars.timestampfield}) from ${dataform.projectConfig.vars.src_schema}.${dataform.projectConfig.vars.src_table} where ${dataform.projectConfig.vars.timestampfield} >= cast(substr('${daylist}',0,10) as Date)
              and ${dataform.projectConfig.vars.timestampfield} < cast(substr('${daylist}',12) as Date)) 
              or ${dataform.projectConfig.vars.timestampfield} <> max(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s) 
            THEN lead(${dataform.projectConfig.vars.timestampfield}) over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc) else max(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s) end as ${dataform.projectConfig.vars.end_at_column_name}, 
            ${dataform.projectConfig.vars.target_hash_unique_col_name},  ${dataform.projectConfig.vars.target_hash_non_unique_col_name}
              from ( 
                select %s, %s, ${dataform.projectConfig.vars.timestampfield}, 
                case 
                when date_diff(lead(${dataform.projectConfig.vars.timestampfield}) over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc), ${dataform.projectConfig.vars.timestampfield} ,DAY)>1 
                then (row_number() over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc) - row_number() over (PARTITION BY %s, %s order by ${dataform.projectConfig.vars.timestampfield} asc))* (-1) 
                else row_number() over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc) - row_number() over (PARTITION BY %s,%s order by ${dataform.projectConfig.vars.timestampfield} asc) end
                as grp,
                TO_BASE64(md5(IFNULL(NULLIF(UPPER(TRIM(CAST(CONCAT(%s)AS STRING))),''),'^^'))) as ${dataform.projectConfig.vars.target_hash_unique_col_name}, 
                TO_BASE64(md5(IFNULL(NULLIF(UPPER(TRIM(CAST(CONCAT(%s)AS STRING))),''),'^^'))) as ${dataform.projectConfig.vars.target_hash_non_unique_col_name}
                from (
                  select DISTINCT * from ${dataform.projectConfig.vars.src_schema}.${dataform.projectConfig.vars.src_table} where  ${dataform.projectConfig.vars.timestampfield} >= cast(substr('${daylist}',0,10) as Date)
               and ${dataform.projectConfig.vars.timestampfield} < cast(substr('${daylist}',12) as Date) 
                  ) t
              where 
              ${dataform.projectConfig.vars.timestampfield} >= cast(substr('${daylist}',0,10) as Date)
               and ${dataform.projectConfig.vars.timestampfield} < cast(substr('${daylist}',12) as Date)
               ) 
               )
              where
                          ${dataform.projectConfig.vars.target_hash_unique_col_name} = a.${dataform.projectConfig.vars.target_hash_unique_col_name} and
              ${dataform.projectConfig.vars.target_hash_non_unique_col_name} = a.${dataform.projectConfig.vars.target_hash_non_unique_col_name}
  
          )
      )  
  """,src_unique,src_non_unique,src_non_unique,src_unique,src_unique,src_non_unique_proc,src_unique,src_non_unique_proc,src_unique,src_unique,src_unique,src_unique,src_non_unique,src_unique,src_unique,src_unique, src_unique, src_unique,src_non_unique_proc, src_unique, src_unique,src_non_unique_proc,src_unique,src_non_unique_proc ,src_non_unique, src_unique, src_unique, src_non_unique_proc, src_unique, src_non_unique_proc, src_unique, src_unique, src_unique, src_unique,src_unique,src_non_unique, src_unique, src_unique, src_unique, src_unique, src_non_unique_proc, src_unique, src_unique, src_non_unique_proc,src_unique, src_non_unique_proc
  )
  `),
  
        ++this.count;
      
  }
  );
  
  var max_count = this.count
  for(this.count = 0; this.count<max_count; ++this.count){
    if (this.count ==0)
    {
      operate("scd2_merge_"+this.count).dependencies("scd2_" + this.count).queries(`
      CREATE OR REPLACE TABLE ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.src_table}_final_table
       PARTITION BY ${dataform.projectConfig.vars.timestampfield}
      as (SELECT *, cast(null as DATE) as min_eff_date_source from ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_scd2_${this.count});
     
     
  `);
  
   }
    else {
  operate("scd2_merge_"+this.count)
  .dependencies("scd2_merge_" + (this.count-1))
  .dependencies("scd2_" + this.count)
  .queries(`
  DECLARE target_unique STRING;
  DECLARE target_non_unique STRING;
  
  
  SET target_unique = (
  WITH selected_columns as (
  SELECT primary_key_column_name  
      FROM dataform_staging.table_to_unique
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}_final_table"  and table_catalog = "latika-experiments" and table_schema = "${dataform.projectConfig.vars.target_schema}"
  )
  SELECT STRING_AGG(primary_key_column_name) AS columns FROM selected_columns
  );
  
  
  SET target_non_unique = (
  WITH selected_columns as (
  SELECT column_name 
      FROM latika-experiments.${dataform.projectConfig.vars.target_schema}.INFORMATION_SCHEMA.COLUMNS
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}_final_table" and column_name not in (SELECT primary_key_column_name  
      FROM dataform_staging.table_to_unique
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}_final_table"  and table_catalog = "latika-experiments" and table_schema = "${dataform.projectConfig.vars.target_schema}"
  )  and column_name <> "min_eff_date_source" 
  )
  SELECT STRING_AGG(column_name) AS columns FROM selected_columns
  );
  
  
  
  UPDATE ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.src_table}_final_table s
  SET s.min_eff_date_source = DATE_ADD(b.min_eff_date, INTERVAL -1 DAY)
  FROM (
    select min(a.min_eff_date) as min_eff_date, ${dataform.projectConfig.vars.target_hash_unique_col_name} from (
  
    select 
  min(${dataform.projectConfig.vars.start_from_column_name}) as min_eff_date, ${dataform.projectConfig.vars.target_hash_unique_col_name} from (select * EXCEPT (min_eff_date_source) from  ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.src_table}_final_table where ${dataform.projectConfig.vars.end_at_column_name} is null 
  union ALL
  select *  EXCEPT (r) from (select *, row_number() over (partition by ${dataform.projectConfig.vars.target_hash_unique_col_name} order by ${dataform.projectConfig.vars.start_from_column_name}) as r from  ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_scd2_${this.count} ) where r < 3 ) group by ${dataform.projectConfig.vars.target_hash_unique_col_name}, ${dataform.projectConfig.vars.target_hash_non_unique_col_name}
   
   except DISTINCT
   select ${dataform.projectConfig.vars.start_from_column_name}, ${dataform.projectConfig.vars.target_hash_unique_col_name} from  ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.src_table}_final_table where ${dataform.projectConfig.vars.end_at_column_name} is null 
  )  a
    group by ${dataform.projectConfig.vars.target_hash_unique_col_name}
  ) b
  WHERE s.${dataform.projectConfig.vars.target_hash_unique_col_name}=b.${dataform.projectConfig.vars.target_hash_unique_col_name} and ${dataform.projectConfig.vars.end_at_column_name} is null;
  
  EXECUTE IMMEDIATE format("""
  MERGE  ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.src_table}_final_table  as T
  USING (SELECT %s
  FROM ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_scd2_${this.count}
  ) AS s
  ON   T.${dataform.projectConfig.vars.target_hash_unique_col_name}  =  s.${dataform.projectConfig.vars.target_hash_unique_col_name}
          AND T.${dataform.projectConfig.vars.target_hash_non_unique_col_name} = s.${dataform.projectConfig.vars.target_hash_non_unique_col_name}
  AND t.${dataform.projectConfig.vars.end_at_column_name} is null 
  WHEN MATCHED AND t.${dataform.projectConfig.vars.end_at_column_name} is null and s.${dataform.projectConfig.vars.start_from_column_name} < (select min(${dataform.projectConfig.vars.start_from_column_name}) from ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_scd2_${this.count} where ${dataform.projectConfig.vars.start_from_column_name} <> (select min(${dataform.projectConfig.vars.start_from_column_name}) from ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_scd2_${this.count}))THEN
  UPDATE SET T.${dataform.projectConfig.vars.end_at_column_name} = T.min_eff_date_source 
  WHEN NOT MATCHED BY SOURCE AND t.${dataform.projectConfig.vars.end_at_column_name} is null THEN
  UPDATE SET ${dataform.projectConfig.vars.end_at_column_name} = T.min_eff_date_source
  WHEN NOT MATCHED BY TARGET THEN
    INSERT(  %s, min_eff_date_source)
    VALUES(%s, null)
  """,target_non_unique, target_non_unique, target_non_unique);
  
   
  `)
    }
  }
  