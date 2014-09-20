CREATE TABLE enron(id string, times string,fromheader String, toheader Array<String>,cc Array<String>,Subject String, context String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
LOCATION 's3n://spring-2014-ds/enron_dataset/';

Create Table enron_to as
select id, times, fromheader enron_to
From enron LATERAL VIEW explode(toheader) etab as enron_to;


insert overwrite directory 's3n://hivedsbucket/result'
select regexp_extract(times,'([a-zA-Z]{3}, )([1-3]?[0-9] )([a-zA-Z]{3} )([0-9]{4} )(.*?)',4), regexp_extract(times,'([a-zA-Z]{3}, )([1-3]?[0-9] )([a-zA-Z]{3} )([0-9]{4} )(.*?)',3), count(distinct(id)) as total_mail  from enron_to 
group by regexp_extract(times,'([a-zA-Z]{3}, )([1-3]?[0-9] )([a-zA-Z]{3} )([0-9]{4} )(.*?)',4), regexp_extract(times,'([a-zA-Z]{3}, )([1-3]?[0-9] )([a-zA-Z]{3} )([0-9]{4} )(.*?)',3);


insert overwrite directory 's3n://hivedsbucket/result'
select rtrim(ltrim(regexp_replace(subject,'RE:|FW:',''))), count(*) from enron
group by rtrim(ltrim(regexp_replace(subject,'RE:|FW:','')));


SELECT s1.fromheader, s1.toheader, count(*) as num
    FROM enron s1
    GROUP BY s1.fromheader, s1.toheader
    ORDER BY num DESC
    LIMIT 5;


select count(fromheader), count(subject) from enron where lower(subject) like 're%' or lower(subject) like 'fw%' group by fromheader, subject;
