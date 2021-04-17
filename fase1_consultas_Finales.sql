/* CONSULTAS PROYECTO BD2 */

/* 1. Consulta que agrupe la cantidad actual de contagios por país. */
SELECT L.name,  SUM(DR.new_cases) as contagios
FROM DailyRecord DR
INNER JOIN Location L
On DR.id_location = L.id_location
GROUP BY L.name
ORDER BY contagios DESC;

/* 2. Crear un stored procedure que reciba el nombre del país y nos muestre el acumulado
mensual de infectados, muertes y vacunados. */
CREATE OR ALTER PROCEDURE acumulado @Pais VARCHAR(20)
AS
	SELECT DATEPART(yy,DR.date) AS y, DATEPART(mm,DR.date) AS  m,  SUM(DR.new_cases) as infectados, 
	SUM (SUM(DR.new_cases)) OVER (ORDER BY DATEPART(yy,DR.date), DATEPART(mm,DR.date)) AS acumuladoInfectados,
	SUM(DR.new_deaths) as muertes,
	SUM (SUM(DR.new_deaths)) OVER (ORDER BY DATEPART(yy,DR.date), DATEPART(mm,DR.date)) AS acumuladoMuertes,
	SUM(DR.new_vaccinations) as vacunados,
	SUM (SUM(DR.new_vaccinations)) OVER (ORDER BY DATEPART(yy,DR.date), DATEPART(mm,DR.date)) AS acumuladoVacunados
	FROM DailyRecord DR
    INNER JOIN Location L
    ON L.id_location = DR.id_location
    WHERE L.name = @Pais
    GROUP BY DATEPART(yy,DR.date), DATEPART(mm,DR.date)
    ORDER BY y, m;
GO

EXEC acumulado @Pais = 'Guatemala';


/* 3. Consulta que agrupe la cantidad actual de contagios de los últimos 3 meses por continente. */
SELECT C.name,  SUM(DR.new_cases) as contagios
FROM DailyRecord DR
INNER JOIN Location L
On DR.id_location = L.id_location
INNER JOIN Continent C
ON L.id_continent = C.id_continent
WHERE DR.date >= DATEADD([month], DATEDIFF([month], '19000101', GETDATE()) - 3, '19000101')
AND DR.date < DATEADD([month], DATEDIFF([month], '19000101', GETDATE()), '19000101')
GROUP BY C.name
ORDER BY contagios DESC;

/* 4. Vista que muestre los países con mayor aceleración de contagios durante el mes de
diciembre 2020 y enero 2021.*/
CREATE OR ALTER VIEW aceleracion
 AS SELECT TOP 10 L.name, AVG(DR.reproduction_rate) AS tasa_promedio
 FROM DailyRecord DR
 INNER JOIN Location L
 ON L.id_location = DR.id_location
 WHERE DR.date >= CAST('2020-12-01' AS datetime)
 AND DR.date <= CAST('2021-01-31' AS datetime)
 GROUP BY L.name
 ORDER BY tasa_promedio DESC;
GO

SELECT * FROM Aceleracion;

/* 5. Promedio contagios durante el primer trimestre de la pandemia. */
SELECT AVG(DR.new_cases) AS promedio_contagios
FROM DailyRecord DR
WHERE DR.date >= CAST('2020-01-01' AS datetime)
AND DR.date <= CAST('2020-03-31' AS datetime);

/* 6. Crear un stored procedure que reciba un rango de infectados por día y devuelva los países
que en algún momento tuvieron ese rango, con su fecha correspondiente. */
CREATE OR ALTER PROCEDURE rango_infectados @Min int, @Max int
AS
    SELECT L.name, DR.date
    FROM DailyRecord DR
    INNER JOIN Location L
    ON L.id_location = DR.id_location
    WHERE DR.new_cases >= @Min
    AND DR.new_cases <= @Max
GO

EXEC rango_infectados @Min = 1, @Max = 5;

/* 7. Crear una vista que muestre al top 10 de países con mayor cantidad de pruebas.*/
CREATE OR ALTER VIEW top_pruebas
 AS SELECT TOP 10 L.name, SUM(DR.new_tests) AS total_tests
 FROM DailyRecord DR
 INNER JOIN Location L
 ON L.id_location = DR.id_location
 GROUP BY L.name
 ORDER BY total_tests DESC;
GO

SELECT * FROM top_pruebas;

/* 8. Crear un stored procedure que reciba la fecha como parámetro y que muestre el país que
reporto más muertes en ese día. */
CREATE OR ALTER PROCEDURE muertes_dia @Fecha VARCHAR(10)
AS
    SELECT TOP 1 L.name, DR.new_deaths
    FROM DailyRecord DR
    INNER JOIN Location L
    ON L.id_location = DR.id_location
    WHERE DR.date = CAST(@Fecha AS datetime)
    ORDER BY DR.new_deaths DESC;
GO

EXEC muertes_dia @Fecha = '2020-12-01';

/* 9. Consulta que muestre los datos de Guatemala para un rango de fechas especifico. */
SELECT *
FROM DailyRecord DR
INNER JOIN Location L
ON L.id_location = DR.id_location
WHERE L.name = 'Guatemala' 
AND DR.date >= CAST('2020-12-01' AS datetime)
AND DR.date <= CAST('2021-01-31' AS datetime);


/*10. Consulta que muestre los paises de Latinoamerica ordenados de los mas infectados a los menos
	  para un rango de fechas en especifico */
SELECT L.name, SUM(DR.new_cases) as infectados
FROM DailyRecord DR
INNER JOIN Location L
ON L.id_location = DR.id_location
WHERE L.name IN ('Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Costa Rica', 
				'Cuba', 'Ecuador', 'El Salvador', 'Guatemala', 'Honduras', 'Mexico', 
				'Nicaragua', 'Panama', 'Paraguay', 'Peru', 'Dominican Republic', 'Uruguay','Venezuela')
AND DR.date >= CAST('2020-12-01' AS datetime)
AND DR.date <= CAST('2021-01-31' AS datetime)
GROUP BY L.name
ORDER BY infectados DESC;

/* FRAGMENTACIÓN Y PAGINACIÓN */
DBCC SHOWCONTIG WITH TABLERESULTS, ALL_INDEXES;  

/* COLLATION */
select t.name table_name, sc.name column_name, sc.collation_name from sys.columns sc
inner join sys.tables t on sc.object_id=t.object_id
where t.name='Location' or t.name='DailyRecord' or t.name='Continent' or t.name='ISO_Code' or t.name='Tests_Unit'

/* BACK UP */
-- El nombre del fichero tendrá este Formato DB_YYYYDDMM.BAK 
DECLARE @name VARCHAR(50) -- Nombre de la Base de Datos
DECLARE @path VARCHAR(256) -- Ruta para las copias de seguridad
DECLARE @fileName VARCHAR(256) -- Nombre del Fichero
DECLARE @fileDate VARCHAR(20) -- Usado para el nombre del fichero
 
-- Ruta para las copias de seguridad
SET @path = '/var/opt/mssql/data/' -- Formato del nombre del fichero
SELECT @fileDate = CONVERT(VARCHAR(20),GETDATE(),112) -- excluir estas bases datos
DECLARE db_cursor CURSOR FOR SELECT name FROM master.dbo.sysdatabases WHERE name NOT IN ('master','model','msdb','tempdb')
 
OPEN db_cursor FETCH NEXT FROM db_cursor INTO @name 
 
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @fileName = @path + @name + '_' + 'bk' + '.BAK'
    BACKUP DATABASE @name TO DISK = @fileName
    FETCH NEXT FROM db_cursor INTO @name
END
 
CLOSE db_cursor
DEALLOCATE db_cursor

/* Copiar del contenedor al host */
-- docker cp 78d917e35cfa:/var/opt/mssql/data/ "C:\Users\diego\Documents\USAC\Bases 2\[BD2]Proyecto_Clase\backup"

/* RESTAURAR DB */
USE [master]
GO
RESTORE DATABASE Covid
FROM DISK = '/var/opt/mssql/data/Covid_bk.BAK'
WITH REPLACE



/* TRUNCADO DE REGISTRO DE TRANSACCIONES */

/* Crear backup de la bitacora de transacciones. */
BACKUP LOG Covid TO DISK = '/home/Backup_Covid_Log.bak';
GO

ALTER DATABASE Covid  
SET RECOVERY SIMPLE;  
GO  
-- Shrink the truncated log file to 1 MB.  
DBCC SHRINKFILE (Covid_log, 1);  
GO  
-- Reset the database recovery model.  
ALTER DATABASE Covid  
SET RECOVERY FULL;  
GO  

--RESTORE LOG Covid FROM DISK = '/home/Backup_Covid_Log.bak'; 