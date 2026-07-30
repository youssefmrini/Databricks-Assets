-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Welcome to SQL Scripting lab

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SQL Scripting is one necessary brick to build Stored Procedures.<br>
-- MAGIC Those will be super helpful in:
-- MAGIC  - Migrating legacy warehouses
-- MAGIC  - Convincing SQL people that the best Data Warehouse is indeed a Lakehouse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Setup

-- COMMAND ----------

use catalog identifier(:catalog);
use schema  identifier(:schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cleanup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In case you want to replay the lab or if you encounter an error about a variable already existing, you can run below cell to clean all variables.

-- COMMAND ----------

drop temporary variable if exists v_TheDate;
drop temporary variable if exists are_you;
drop temporary variable if exists v_output;
drop temporary variable if exists v_bool;
drop temporary variable if exists v_var1;
drop temporary variable if exists v_var2;
drop temporary variable if exists v_TableName;
drop temporary variable if exists v_formating1;
drop temporary variable if exists v_formating2;
drop temporary variable if exists v_counter;
drop temporary variable if exists v_mult;
drop temporary variable if exists a;
drop temporary variable if exists v_cond;
drop temporary variable if exists v_message;
drop temporary variable if exists v_state;
drop temporary variable if exists v_line;
drop temporary variable if exists v_log;
drop temporary variable if exists v_sql;
drop temporary variable if exists v_sql_select;
drop temporary variable if exists v_sql_from;
drop temporary variable if exists v_sql_where;
drop temporary variable if exists v_sql_unpivot;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Compound Statements

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SQL Scripting is all about embedding code inside compound statements (sometime called blocks). <br> They are identified with a **BEGIN** / **END** instruction.

-- COMMAND ----------

begin
  declare v_TheDate timestamp default current_timestamp;
  select v_TheDate;
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When using SQL Scripting, your code **must** start with a BEGIN instruction.

-- COMMAND ----------

-- This cell will fail !!  (first instruction is not begin)
declare v_TheDate timestamp default current_timestamp;
begin
  select v_TheDate;
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Compound statements can live inside other compound statements.

-- COMMAND ----------

begin
  declare v_TheDate timestamp default current_timestamp;
  begin
    begin
      select v_TheDate;
    end;
  end;
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Except the most outer one, any compound statements can have a label.

-- COMMAND ----------

begin
  declare v_TheDate timestamp default current_timestamp;
  lbl_block1:
  begin
    lbl_block2:
    begin
      set v_TheDate = add_months(v_TheDate::date, -1)::timestamp;
    end lbl_block2;
  end lbl_block1;
  values (v_TheDate);
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can't close a compound statements intricated in another one... thanksfully!

-- COMMAND ----------

-- This cell will fail !! ( Error order of label closing )
begin
  declare v_TheDate timestamp default current_timestamp;
  lbl_block1:
  begin
    lbl_block2:
    begin
      select v_TheDate;
    end lbl_block1;     -- I've switched end of block1 and block2
  end lbl_block2;
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Identation is super helpful (please do it) but **is not required** (beware of the nasty devs).<br>
-- MAGIC Proper end of statements with semi-colons and end of flow controls are mandatory.

-- COMMAND ----------

-- super bad indentation practice... that still works
begin declare are_you string default
'able to work like this?'
; lbl_block1:begin lbl_block2:
                       begin
select are_you; end lbl_block2; end 
     lbl_block1;                 end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Flow logic 
-- MAGIC Labels help to keep code organized and also to perform some operations on those.
-- MAGIC
-- MAGIC First run   : v_bool = True 
-- MAGIC Second run  : v_bool = False ( swap comments on lines 5 and 6 ) 

-- COMMAND ----------

begin
  declare v_output string  default '';

 -- try with both true and false
  -- declare v_bool   boolean default true;      -- comment it for second run
  declare v_bool   boolean default false;  -- uncomment it for second run
  lbl_block1:
  begin
    lbl_block2:
    begin
      if v_bool then leave lbl_block2; end if;
      set  v_output = v_output || 'block 2';
      leave lbl_block1;
    end lbl_block2;
    set  v_output = v_output || 'block 1';
  end lbl_block1;
  select v_output;
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Variables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can declare variables with all information or let the engine infer the data type for you.<br>
-- MAGIC A variable declared inside SQL Scripting is a local variable, a variable declared outside SQL Scripting is a session variable.<br>
-- MAGIC Point of interest, a local variable is a session variable that get dropped at the end of the SQL Scripting.<br>
-- MAGIC Outside SQL Scripting you have to use session variable syntax in the set statement to avoid confusion with the Spark set command.

-- COMMAND ----------

begin
  declare v_var1 string default '1';
  declare v_var2 = '2';

  set v_var1 = (select v_var1 || '1');
  set v_var2 = v_var2 || '2';

  values (v_var1, v_var2);
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Variables declared in SQL Scripting are cleaned after the main compound statements is ended.

-- COMMAND ----------

-- this will fail
values (v_var1, v_var2);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Below is the session variable, that you can use inside SQL Scripting but those variables will be converted to SQL Scripting variables.

-- COMMAND ----------

declare variable v_var1 string default '1';
set variable v_var1 = (select v_var1 || '1');
values (v_var1);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Below is the SQL Scripting variable syntax, that will get converted to session variable.<br>
-- MAGIC You can't just use set, you need to use set variable outside of SQL Scripting.
-- MAGIC
-- MAGIC 1st run : line 3 uncommented -> Will fail ( need set variable syntax - outside a compount statement ) <br>
-- MAGIC 2nd run : line 3 commented, line 4 uncommented -> Will work

-- COMMAND ----------

drop temporary variable if exists v_var2; 
declare v_var2 = '2';
-- set v_var2 = v_var2 || '2';              -- This will fail, then comment it
set variable v_var2 = v_var2 || '2';  -- Uncomment
values (v_var2);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now we have declared both variables v_var1 and v_var2 as session variables, they remain accessible.

-- COMMAND ----------

values (v_var1, v_var2);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As those variables now exists as session variable, you can't re-declare them in SQL Scripting.

-- COMMAND ----------

-- this cell will fail
begin
  declare v_var1 = '1';
  declare v_var2 = '2';
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You must work around with the declare or replace syntax.

-- COMMAND ----------

begin
  declare or replace v_var1 = '1';
  declare or replace v_var2 = '2';
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now the session variables has been replaced by local variables, they no longer exists as session variables because they were automatically dropped at the end of the cell execution

-- COMMAND ----------

-- this cell will fail
values (v_var1, v_var2);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. SQL Statements

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We've already seen two SQL Statements with the DECLARE and SET for variables.<br>
-- MAGIC You can add any SQL query, which are proper DDL, DML or select/values statements.<br>
-- MAGIC
-- MAGIC 1st run : line 6 commented, line 5 uncommented <br>
-- MAGIC 2nd run : line 6 uncommented, line 5 commented

-- COMMAND ----------

begin
  declare v_TableName string;
  
  -- try with both true and false
  declare v_bool   boolean default true;      -- comment it for second run
  -- declare v_bool   boolean default false;  -- uncomment it for second run

  if v_bool
    then
    -- Creating a table
      set v_TableName = 't_true_table';
      create or replace table identifier(v_TableName) (col string);
      insert into identifier(v_TableName) (col) values (replace(v_TableName, 't_', 'col_'));
    else
    -- Creating a view
      set v_TableName = 'v_false_view';
      create or replace view identifier(v_TableName) as select 'col_false_view' as col;
  end if;
  
  select col from identifier(v_TableName);
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Be aware that we're only displaying the last resultset from select / values.

-- COMMAND ----------

begin
  values ("You won't see this.");
  values ("But you'll see this.");
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When working with variables, you can apply functions directly in the script without leveraging a query statement, even if it's valid.<br>
-- MAGIC Keep the select when you need to read from a table.

-- COMMAND ----------

begin
  declare v_formating1 = array('20250311', '20250312', '20250313');
  declare v_formating2 = v_formating1;
  
  set v_formating1 = (select array_agg(to_char(timestampadd(hour, right(v, 2)::int, to_date(v, 'yyyyMMdd')), 'yyyy.dd.MM HH:mm'))
                        from explode(v_formating1) as t (v));
  
  -- Better to go for the direct approach
  set v_formating2 = transform(v_formating2, v -> to_char(timestampadd(hour, right(v, 2)::int, to_date(v, 'yyyyMMdd')), 'yyyy.dd.MM HH:mm'));
  
  values (v_formating1, v_formating2);
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Branching

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Imagine any data pipeline. Branching based on some conditions is super important because you may need to do different operations based on the inputs.<br>
-- MAGIC The most popular that you'll see everywhere is the IF branching, CASE is another form of branching.<br>

-- COMMAND ----------

begin
  if 1 > 2
    then values ('1>2');
  elseif 1 = 2
    then values ('1=2');
  else
    values ('1<2');
  end if;
end;

-- COMMAND ----------

begin
  case
    when 1 > 2
    then values ('1>2');
    when 1 = 2
    then values ('1=2');
    else values ('1<2');
  end case;
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _Nota Bene_: Branching can't have labels.

-- COMMAND ----------

-- this cell will fail
begin
  lbl_if: if true then values ('a') end if lbl_if;
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Looping

-- COMMAND ----------

-- MAGIC %md
-- MAGIC There are multiple flow statements to loop over something.<br>
-- MAGIC LOOP, WHILE, REPEAT, and FOR. This latter one is also the most popular.<br>
-- MAGIC LOOP is piloted with LEAVE and ITERATE statements, don't forget to put an exit condition.

-- COMMAND ----------

begin
  declare v_counter = 1;
  lbl_loop:
  loop
    
    if v_counter = 3 then
      set v_counter = v_counter + 10;
      iterate lbl_loop;
    end if;
    
    if v_counter > 5 then leave lbl_loop; end if;
    
    set v_counter = v_counter + 1;
  end loop lbl_loop;
  values (v_counter);
end;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC WHILE and REPEAT have an exit condition => WHILE exit condition is evaluated at the start, REPEAT at the end.

-- COMMAND ----------

-- While loop ( exit evaluated at the start )
begin
  declare v_counter = 1;
  while v_counter <= 3
  do
    set v_counter = v_counter + 1;
  end while;
  values (v_counter);
end;

-- COMMAND ----------

-- repeat loop ( exit evaluated at the end )
begin
  declare v_counter = 1;
  repeat
    set v_counter = v_counter + 1;
  until v_counter > 3
  end repeat;
  values (v_counter);
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC FOR loop can leverage any query, order by included. Beware, this will get processed on a **row-by-row** basis.<br>
-- MAGIC **Never convert a join to a FOR loop unless you have very specific reasons.**

-- COMMAND ----------

begin
  declare v_mult = 1;
  
  lbl_for_loop:
  for c as
  (
    select id
      from range(1,10)
  order by id desc
  )
  do
    case
      when c.id <= 4
      then set v_mult = v_mult * power(2, c.id);
      else set v_mult = v_mult * c.id;
    end case;
  end for lbl_for_loop;
  
  values (v_mult);
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6. Handling errors and conditions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Handling errors and conditions is very important in managing pipelines, for logging and reporting but also triggering specific actions.<br>
-- MAGIC

-- COMMAND ----------

begin
  declare or replace a = 1;
  
  declare exit handler for divide_by_zero -- comment and retry ( divide by zero error )
  begin
    values ('Exit');
  end;
  
  set a = 10;
  set a = a / 0;
  values (a);
end;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's do something more useful with this handler with logging.

-- COMMAND ----------

begin
  declare exit handler for divide_by_zero
  begin
    declare v_cond    string;
    declare v_message string;
    declare v_state   string;
    declare v_line    bigint;
    declare v_log     string;
    
    create or replace table log (eventtime timestamp, log string);
    
    get diagnostics condition 1
      v_cond    = condition_identifier,
      v_state   = returned_SQLState,
      v_line    = line_number,
      v_message = message_text;
    
    set v_log = 'Condition: ' || v_cond    || '\n'
             || 'SQLSTATE: '  || v_state   || '\n'
             || 'Line: '      || v_line    || '\n'
             || 'Message: '   || v_message;
    insert into log (eventtime, log) values (current_timestamp(), v_log);
    
    select eventtime, log from log;
  end;

  -- Provoke the error
  select 10/0;
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can refer any of the listed [Error Classes](https://docs.databricks.com/aws/en/error-messages/error-classes) and [SQLSTATE error codes](https://docs.databricks.com/aws/en/error-messages/sqlstates).<br>

-- COMMAND ----------

begin
  declare exit handler for WKT_PARSE_ERROR
  begin
    values ('Another Error');
  end;
  
  select h3_polyfillash3('POINT EMPTY', 2);
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Features still under development**<br>
-- MAGIC In the future:
-- MAGIC - The handler will be able to continue to process the SQL Scripting instead of only exiting.<br>
-- MAGIC - The RESIGNAL instruction will allow to re-throw the error to an outsider call.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7. Examples

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 7a : Administrative tasks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Combination of FOR loops and execute immediate are awesome for DBAs.<br>
-- MAGIC In this scenario, customer wants to convert DBx in-house RLS to a layer of views because he needs to delta share some tables.<br>
-- MAGIC We're using a string variable to build another query that will be executed with the execute immediate instruction.

-- COMMAND ----------

-- cleanup
drop table if exists FilteredTable;
create table FilteredTable
( id    int
, val   string
);

-- COMMAND ----------

--setup 
insert into FilteredTable values (1, 'A'), (2, 'B');

-- COMMAND ----------

create or replace function f_rls_filter (p_val  string)
returns boolean
return case p_val when 'A' then true else false end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Function is not yet applied, you'll see both rows

-- COMMAND ----------

select * from FilteredTable;

-- COMMAND ----------

alter table FilteredTable set row filter f_rls_filter on (val);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Function has been applied so you'll see one row

-- COMMAND ----------

select * from FilteredTable;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC First check the values of both variabilized queries, then comment the values and uncomment the execute immediate.

-- COMMAND ----------

 begin
  declare or replace v_sql_view  string;
  declare or replace v_sql_alter string;

  lbl_for_loop:
  for c as
  (
  select table_name, filter_name, target_columns
    from system.information_schema.row_filters
   where table_catalog = current_catalog()
     and table_schema  = current_schema()
     and table_name    = 'FilteredTable' collate unicode_ci
  )
  do
    set v_sql_view  = "create or replace view identifier('v_rls_" || c.table_name || "') as\n"
                   || "select * from identifier('" || c.table_name || "')\n"
                   || " where " || c.filter_name || "(" || c.target_columns || ")";
    set v_sql_alter = "alter table identifier('" || c.table_name || "') drop row filter";
    values (v_sql_view, v_sql_alter);
   -- execute immediate v_sql_view;    -- uncomment this to execute
   -- execute immediate v_sql_alter;   -- uncomment this to execute
  end for lbl_for_loop;
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC RLS has been removed from the table:

-- COMMAND ----------

select * from FilteredTable;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC But a RLS view has been created:

-- COMMAND ----------

select * from v_rls_FilteredTable;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Another scenario would be to loop over all your unclustered tables then perform an alter to apply automatic liquid clustering.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 7b : Optimization

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In this scenario your customer is constrained in its cluster choice and they can't scale it.<br>
-- MAGIC They have a big aggregation of sales data that spills to disk significantly.
-- MAGIC
-- MAGIC Do you remember when I wrote "Never convert a join to a FOR loop unless very specific reasons".<br>
-- MAGIC This is one of the specific reason.

-- COMMAND ----------

create or replace table MySalesAggTbl
( Solddate          date
, ss_store_sk       integer
, MAU               integer
, ItemDst           integer
, DemoDst           integer
, ss_wholesale_cost decimal(20,2)
, insert_ts         timestamp default current_timestamp()
)
tblproperties ('delta.feature.allowColumnDefaults' = 'supported');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Code of query below is not particulary relevant - goal here is to make the query spill to disk.<br>
-- MAGIC Query should last around 4.5 minutes - you can skip over if you want.

-- COMMAND ----------

insert overwrite MySalesAggTbl (Solddate, ss_store_sk, MAU, ItemDst, DemoDst, ss_wholesale_cost)
  select dt.d_date
       , ss.ss_store_sk
       , count(ss.ss_customer_sk)
       , count(ss.ss_item_sk    )
       , count(ss.ss_cdemo_sk   )
       , sum(ss.ss_wholesale_cost + sq.id)
    from tpcds.sf_100_liquid.store_sales   as ss
    join tpcds.sf_100_liquid.date_dim      as dt on dt.d_date_sk = ss.ss_sold_date_sk
    join lateral explode(sequence(0,9999)) as sq (id)
   where dt.d_dom  =   15
     and dt.d_moy  <    9
     and dt.d_year = 1999
group by dt.d_date
       , ss.ss_store_sk;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can actually split the query into smaller queries, putting the logic of the dates in the FOR loop, avoiding a join and dealing with less data each day to avoid spill.<br>
-- MAGIC Note we can track the total number of rows impacted leveraging the GET DIAGNOSTICS command.<br>
-- MAGIC Useful for built-in frameworks that logs steps.<br>
-- MAGIC SQL Script below should last less than 4 minutes - you can skip over if you want.
-- MAGIC
-- MAGIC _Nota Bene_: With larger datasets that would spill more, the performance improvement would be better.<br>
-- MAGIC I didn't want to make those steps last too long.

-- COMMAND ----------

begin
  declare or replace variable v_rcnt       integer        default 0;
  declare or replace variable v_rcnt_total array<integer> default array()::array<integer>;
  
  truncate table MySalesAggTbl;
  
  lbl_for_loop:
  for c as
  (
    select d_date, d_date_sk
      from tpcds.sf_100_liquid.date_dim
     where d_dom  =   15
       and d_moy  <    9
       and d_year = 1999
  order by 1 asc
  )
  do
    
    insert into MySalesAggTbl (Solddate, ss_store_sk, MAU, ItemDst, DemoDst, ss_wholesale_cost)
      select c.d_date
           , ss.ss_store_sk
           , count(ss.ss_customer_sk)
           , count(ss.ss_item_sk    )
           , count(ss.ss_cdemo_sk   )
           , sum(ss.ss_wholesale_cost + sq.id)
        from tpcds.sf_100_liquid.store_sales   as ss
        join lateral explode(sequence(0,9999)) as sq (id)
       where ss.ss_sold_date_sk = c.d_date_sk
    group by ss.ss_store_sk;
    
    get diagnostics v_rcnt = row_count;
    set variable v_rcnt_total = array_append(v_rcnt_total, v_rcnt);
    
  end for lbl_for_loop;
  
  set variable v_rcnt = aggregate(v_rcnt_total, 0, (acc, x) -> acc + x);
  values (v_rcnt);
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 7c : Query building

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SQL Scripting is also an excellent tool to build queries.<br>
-- MAGIC In the following scenario, we have families with children.<br>
-- MAGIC An association wants to bring **exactly one child per family** to the beach.<br>
-- MAGIC They would like to know what are **all their options**?<br>
-- MAGIC Format-wise they expect three columns: GroupNumber, Family and Child.
-- MAGIC
-- MAGIC The problem here is the way to build the query depends on the number of families, we want to "cross join" all families children.

-- COMMAND ----------

create or replace table FamilyAndChildren
( Family    string
, Child     string
, ChildId   tinyint
);

insert into FamilyAndChildren (Family, Child, ChildId) values
('Family1', 'Carl'   , 1),
('Family1', 'Tyagi'  , 2),
('Family2', 'Jan'    , 1),
('Family2', 'Claudio', 2),
('Family2', 'Mieke'  , 3);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here is one possible query to answer the question:

-- COMMAND ----------

with cte_data as
(
select monotonically_increasing_id() + 1 as GroupNum
     , fm1.Child as c1
     , fm2.Child as c2
  from FamilyAndChildren as fm1
  join FamilyAndChildren as fm2
 where fm1.Family = 'Family1'
   and fm2.Family = 'Family2'
)
  select *
    from cte_data
 unpivot (Child for Family in ( c1 as Family1
                              , c2 as Family2
                              )) as upv
order by 1, 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's add new families:

-- COMMAND ----------

insert into FamilyAndChildren (Family, Child, ChildId) values
('Family3', 'Youri'  , 1),
('Family4', 'Dirk'   , 1),
('Family4', 'Luisa'  , 2),
('Family5', 'Fred'   , 1),
('Family5', 'Lillo'  , 2),
('Family5', 'Oliver' , 3),
('Family6', 'Erhan'  , 1),
('Family6', 'Olivier', 2);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You then need to adapt above query, which is not something you want to do manually.<br>
-- MAGIC First, test the query generated in the v_sql variable.<br>
-- MAGIC Then comment the values and uncomment the execute immediate to get the results.

-- COMMAND ----------

begin
  declare v_sql = 'with cte_data as\n(\n'
                || 'select monotonically_increasing_id() + 1 as GroupNum' || '\n';
  declare v_sql_select  string default '';
  declare v_sql_from    string;
  declare v_sql_where   string default ' where true' || '\n';
  declare v_sql_unpivot string default ')\n  select *\n    from cte_data\n  unpivot (Child for Family in ( ';

  for c as
  (
    select Family
         , row_number() over(order by Family) as FamilyNum
      from FamilyAndChildren
  group by Family
  order by FamilyNum asc
  )
  do
    set v_sql_select = v_sql_select || '     , fm' || c.FamilyNum || '.Child as c' || c.FamilyNum || '\n';
    set v_sql_where = v_sql_where || '   and fm' || c.FamilyNum || '.Family = "' || c.Family || '"' || '\n';

    case c.FamilyNum
      when 1
      then set v_sql_from = '  from FamilyAndChildren as fm1' || '\n';
           set v_sql_unpivot = v_sql_unpivot || 'c' || c.FamilyNum || ' as Family' || c.FamilyNum || '\n';
      else set v_sql_from = v_sql_from || '  join FamilyAndChildren as fm' || c.FamilyNum || '\n';
           set v_sql_unpivot = v_sql_unpivot || repeat(' ', 31) || ', c' || c.FamilyNum || ' as Family' || c.FamilyNum || '\n';
    end case;
    
  end for;

  set v_sql = v_sql || v_sql_select || v_sql_from || v_sql_where || v_sql_unpivot
           || repeat(' ', 31) || ')) as upv' || '\n'
           || 'order by 1, 2';
  values (v_sql);
  -- execute immediate v_sql; -- uncomment this to execute
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 7d : Recursivity

-- COMMAND ----------

-- MAGIC %md
-- MAGIC DBSQL does not (yet) support recursivity, but we can leverage SQL Scripting as a workaround.<br>
-- MAGIC Here is some data:

-- COMMAND ----------

create or replace table Hierarchy
( NodeID     tinyint
, ParentID   tinyint
, NodeName   string
);

-- COMMAND ----------

insert into Hierarchy (NodeID, ParentID, NodeName) values 
(0,    1, 'City'   ),
(1,    4, 'London' ),
(2,    3, 'Munich' ),
(3,    5, 'Germany'),
(4,    5, 'UK'     ),
(5,    6, 'Europe' ),
(6, null, 'World'  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Congrats, as you made it this far !!! <br> 
-- MAGIC
-- MAGIC Here is a new feature coming soon ( temp tables ) :-)<br>
-- MAGIC Create or replace doesn't work yet with temp tables, so currently it needs to be dropped before re-creation.<br>
-- MAGIC Below cell is there just in case.

-- COMMAND ----------

-- cleanup
drop table if exists HierarchyResult;

-- COMMAND ----------

create temporary table HierarchyResult
( AncestorID      tinyint
, AncestorName    string
, ParentId        tinyint
, ParentName      string
, NodeID          tinyint
, NodeName        string
, NodePath        array<string>
, DepthDiff       tinyint
)
cluster by (DepthDiff);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We're leveraging the repeat looping, checking after every recursion if any work was done.<br>
-- MAGIC We're writing data in the temp table at every step.

-- COMMAND ----------

begin
    -- Variable to control the loop depth
    declare or replace v_DepthDiff = 0;
    declare or replace v_rcnt = 0;

    -- Recursion Initialisation
    insert overwrite HierarchyResult (AncestorID, AncestorName, NodeID, NodeName, NodePath, DepthDiff)
    select NodeID, NodeName, NodeID, NodeName, array(NodeName), v_DepthDiff
      from Hierarchy
     where ParentId is null;
    
    repeat
      insert into HierarchyResult (AncestorID, AncestorName, ParentId, ParentName, NodeID, NodeName, NodePath, DepthDiff)
      select r.AncestorID
           , r.AncestorName
           , r.NodeID
           , r.NodeName
           , h.NodeID
           , h.NodeName
           , array_append(r.NodePath, h.NodeName)
           , v_DepthDiff + 1
        from HierarchyResult as r
        join Hierarchy       as h on h.ParentId = r.NodeID
       where r.DepthDiff = v_DepthDiff;
      
      get diagnostics v_rcnt = row_count;
      set v_DepthDiff = v_DepthDiff + 1;

    until v_rcnt = 0
    end repeat;
    
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's check the results querying the temp table:

-- COMMAND ----------

  select AncestorID, AncestorName, ParentId, ParentName
       , NodeID, NodeName, NodePath, DepthDiff
       , get(NodePath, 1) as lvl_1
       , get(NodePath, 2) as lvl_2
       , get(NodePath, 3) as lvl_3
    from HierarchyResult
order by NodePath;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 7e : More PrPr snippets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This one will be a banger...
-- MAGIC
-- MAGIC but shhhhhhh (it's a secret :p)

-- COMMAND ----------

drop procedure if exists p_display;

-- COMMAND ----------

create procedure p_display (p_value string)
language sql
as
begin
  values (p_value);
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ...once the development will be completed!

-- COMMAND ----------

-- cell will fail ( comming soon )
call p_display('SQL Everywhere');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Thank you!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For assisting this SQL Scripting lab.<br>
-- MAGIC I hope you enjoyed it and that the pieces of code showed here are relevant in your DBSQL knowledge.