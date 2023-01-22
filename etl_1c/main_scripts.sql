/*Создаем базу данных dwh*/
create database dwh;


/*Создаем схемы в БД dwh*/
create schema if not exists src;
create schema if not exists stage;
create schema if not exists datamart;


/*Создаем таблицы для загрузки данных src слоя*/
create or replace procedure src.src_create_tables()
as $$
	begin
         create table if not exists src.nomenclature (nomenclature varchar(250) not null);

         create table if not exists src.contracts (contract varchar(50) not null,
                                                   counterparty varchar(100) not null,
                                                   type_contract varchar(50),
                                                   currency varchar(25),
                                                   company varchar(100) not null);
                                   
         create table if not exists src.counterparties (company varchar(100) not null,
                                                        taxpayer_identification_number varchar(12),
                                                        full_name_company varchar(250) not null);
                                            
         create table if not exists src.sales (dt timestamp not null,
		                                       company varchar(100) not null,
		                                       counterparty varchar(100) not null,
		                                       contract varchar(100) not null,
		                                       nomenclature varchar(250) not null,
		                                       quantity integer not null,
		                                       amount integer not null);
	end;
$$ language plpgsql;

call src.src_create_tables();


/*Загружаем датасеты в БД*/

                                    
/*Создаем таблицы stage слоя*/
create or replace procedure stage.stage_create_tables()
as $$
	begin
		create table if not exists stage.nomenclature (id serial primary key, 
                                                       nomenclature varchar(250) not null,
			                                           dt_create timestamp not null default now(),
			                                           dt_update timestamp not null default now(),
			                                           is_actual boolean not null default true);
                                                                                          
		create table if not exists stage.contracts (id serial primary key,
                                                    contract varchar(50) not null,
                                                    counterparty varchar(100) not null,
                                                    company varchar(100) not null,
                                                    dt_create timestamp not null default now(),
                                                    dt_update timestamp not null default now(),
                                                    is_actual boolean not null default true);
                                          
		create table if not exists stage.counterparties (id serial primary key,
                                                         company varchar(100) not null,
                                                         taxpayer_identification_number varchar(12),
                                                         full_name_company varchar(250) not null,
                                                         dt_create timestamp not null default now(),
                                                         dt_update timestamp not null default now(),
                                                         is_actual boolean not null default true);
		                                                                                      
		create table if not exists stage.sales (id serial primary key,
                                                dt timestamp not null,
                                                company integer references stage.counterparties (id),
                                                counterparty integer references stage.counterparties (id),
                                                contract integer references stage.contracts (id),
                                                nomenclature integer references stage.nomenclature (id),
                                                quantity integer not null,
                                                amount integer not null,
                                                dt_create timestamp not null default now(),
                                                dt_update timestamp not null default now(),
                                                is_actual boolean not null default true);                                          

 	end;
$$ language plpgsql;

call stage.stage_create_tables();
                                                
/*Заполняем таблицы в stage слое*/
create or replace procedure stage.stage_insert_data()
as $$
	begin
		insert into stage.nomenclature (nomenclature) 
		select * from src.nomenclature;
 
		insert into stage.contracts (contract,counterparty,company) 
		select contract,counterparty,company from src.contracts;

		insert into stage.counterparties (company,taxpayer_identification_number,full_name_company)
		select company,taxpayer_identification_number,full_name_company from src.counterparties;
                                       
		insert into stage.sales (dt,company,counterparty,contract,nomenclature,quantity,amount)                                 
		select s.dt, 
		      c1.id as company, 
		      c2.id as counterparty, 
		      c3.id as contract, 
		      n.id as nomenclature, 
		      s.quantity, 
		      s.amount  
		from src.sales s left join stage.counterparties c1 on s.company = c1.company
		                 left join stage.counterparties c2 on s.counterparty = c2.company
		                 left join stage.contracts c3 on (s.contract = c3.contract 
		                                                  and s.company = c3.company 
		                                                  and s.counterparty = c3.counterparty)
		                 left join stage.nomenclature n on s.nomenclature = n.nomenclature;
		                 
 	end;
$$ language plpgsql;		                 

call stage.stage_insert_data();


/*Формируем таблицу в слое datamart*/
create or replace view datamart.report as
	select c1.company as company,
	       c2.company as counterparty,
	       c3.contract as contract,
	       n.nomenclature as nomenclature,
	       sum(s.quantity) as total_quantity,
	       sum(s.amount) as total_amount
	from stage.sales s left join stage.counterparties c1 on s.company = c1.id
	                   left join stage.counterparties c2 on s.counterparty = c2.id
	                   left join stage.contracts c3 on s.contract = c3.id
					   left join stage.nomenclature n on s.nomenclature = n.id
	where s.is_actual is true
	group by 1,2,3,4; 

	
/*Проверка отчета*/
select * from datamart.report r limit 10;


/*Очищаем таблицы слоя src*/
truncate table src.contracts;
truncate table src.counterparties;
truncate table src.nomenclature;
truncate table src.sales;


/*Загружаем новые датасеты в БД*/


/*Обновляем таблицы в stage слое*/
create or replace procedure stage.stage_update_data(update_type varchar(7) default 'append',
                                                    dt_range_start date default '1900-01-01',
                                                    dt_range_end date default '1900-01-01')
as $$
	declare current_dt_update timestamp := now();
	begin
		-- Таблица Номенклатура		    		
		merge into stage.nomenclature as target_tbl
		using src.nomenclature as source_tbl 
		on target_tbl.nomenclature = source_tbl.nomenclature
		when matched then 
			update set dt_update = current_dt_update
		when not matched then
			insert (nomenclature,dt_create,dt_update) 
			values (source_tbl.nomenclature,current_dt_update,current_dt_update);
		
		update stage.nomenclature as target_tbl set is_actual = false where target_tbl.dt_update < current_dt_update;
		
		-- Таблица Контракты	
	    merge into stage.contracts as target_tbl
	    using src.contracts as source_tbl 
	    on (target_tbl.contract = source_tbl.contract 
	        and target_tbl.counterparty = source_tbl.counterparty
	        and target_tbl.company = source_tbl.company)
	    when matched then 
	    	update set dt_update = current_dt_update
	    when not matched then
	    	insert (contract,counterparty,company,dt_create,dt_update) 
	    	values (source_tbl.contract, source_tbl.counterparty, source_tbl.company, 
	                current_dt_update, current_dt_update);
	                                                                       
	    update stage.contracts  as target_tbl set is_actual = false where target_tbl.dt_update < current_dt_update;
	   
	    -- Таблица Контрагенты
	    merge into stage.counterparties as target_tbl
	    using src.counterparties as source_tbl 
	    on target_tbl.company = source_tbl.company
	    when matched then
	   	 	update set taxpayer_identification_number = source_tbl.taxpayer_identification_number,
	                   full_name_company = source_tbl.full_name_company,
	                   dt_update = current_dt_update	    
	    when not matched then
	    	insert (company,taxpayer_identification_number,full_name_company,dt_create,dt_update) 
	    	values (source_tbl.company,source_tbl.taxpayer_identification_number,source_tbl.full_name_company,
	                current_dt_update,current_dt_update);
	    
	    update stage.counterparties  as target_tbl set is_actual = false where target_tbl.dt_update < current_dt_update;        
	    
	    --- Таблица Продажи ---
	    if update_type = 'replace' then
	    	--delete from stage.sales s 
	    	--where date(s.dt) >= date(dt_range_start) and date(s.dt) <= date(dt_range_end);
	        update stage.sales s set is_actual = false 
	        where date(s.dt) >= date(dt_range_start) and date(s.dt) <= date(dt_range_end);
	    end if;
	   	
	    insert into stage.sales (dt,company,counterparty,contract,nomenclature,quantity,amount)                                 
		select s.dt, 
		      c1.id as company, 
		      c2.id as counterparty, 
		      c3.id as contract, 
		      n.id as nomenclature, 
		      s.quantity, 
		      s.amount  
		from src.sales s left join stage.counterparties c1 on s.company = c1.company
		                 left join stage.counterparties c2 on s.counterparty = c2.company
		                 left join stage.contracts c3 on (s.contract = c3.contract 
		                                                  and s.company = c3.company 
		                                                  and s.counterparty = c3.counterparty)
		                 left join stage.nomenclature n on s.nomenclature = n.nomenclature;
	   
	end;
$$ language plpgsql; 

call stage.stage_update_data();


