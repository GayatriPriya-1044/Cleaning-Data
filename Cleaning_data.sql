select *
from world_layoffs.layoffs;


-- 1.Deleting Duplicates

-- creating table with same columns as layoffs to prevent from losing data (safe-side)
create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

-- copying data from original table

insert layoffs_staging 
select *
from layoffs;

select *,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country) as row_num
from layoffs_staging;





-- creating new table to check if there exists any duplicates by giving row number
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- inserting data along with row_number to delete duplicates.
insert layoffs_staging2
select *,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country) as row_num
from layoffs_staging;
-- deleting 
delete
from layoffs_staging2
where row_num>1;
-- checking
select *
from layoffs_staging2
where row_num>1;



-- 2.Standardizing the data

select *
from layoffs_staging2;

-- checking if trimming required
select company,trim(company)
from layoffs_staging2;

-- trimming the spaces 
update layoffs_staging2
set company=trim(company);

select distinct country
from layoffs_staging2;

-- if difference is shown by '.' then
update layoffs_staging2
set country='United States'
where country like 'United States%';

select distinct industry
from layoffs_staging2;

-- since crypto and crypto currency is same making everything same
update layoffs_staging2
set industry='Crypto Currency' 
where industry like 'Crypto';

-- trying to fill known data
-- In some companies industries is given as null but has some name in another row so I am trying to copy this data


-- Initially making all blanks into null to avoid confusion

update layoffs_staging2
set industry = null 
where industry='';

select t1.industry,t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2 
	on t1.company=t2.company
    where t1.industry is null and t2.industry is not null;
    
-- updating table accordingly


update layoffs_staging2 as t1
join layoffs_staging2 as t2 
	on t1.company=t2.company
set t1.industry=t2.industry
    where t1.industry is null and t2.industry is not null;

-- checking and found one company with null as industry

select *
from layoffs_staging2
-- where industry is null;
where location = 'Providence';

-- confirmed that it has no known industry value

-- felt like if a company doesnt have total_laid_off and percentage_laid_off its not needed in table.

select *
from layoffs_staging2
where percentage_laid_off is null 
and total_laid_off is null;

-- deleting this unwanted rows

delete 
from layoffs_staging2
where percentage_laid_off is null 
and total_laid_off is null; 

select *
from layoffs_staging2;

-- updating rows and columns 

-- a.deleting row_num column


alter table layoffs_staging2
drop column row_num;

-- b.Changing type of date from text to date(inbuilt)

-- checking if it works
select `date`,str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

-- updating the date type alter

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');


-- checking table one last time
select *
from layoffs_staging2;

-- completed

drop table layoffs_staging;

rename table layoffs_staging2 to layoffs_staging;





















