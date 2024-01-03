import psycopg2
import pandas as pd
import pymssql
import sqlalchemy
from sqlalchemy import text
import time


inicio = time.time()

nombres_colegio ={
        "Colegio Pedagógico de las Américas ": "Colegio Pedagógico Américas",
        "Instituto México Inglés": "IMI",
        "Colegio Mater Dei": "Mater Dei",
        "Universidad UNIVER Bachillerato": "UNIVER Nayarit Bachillerato",
        "Universidad UNIVER de Nayarit":"UNIVER Nayarit",
        "Universidad Tec de Oriente":"Universidad Tec Oriente",
        "Univer Durango":"UNIVER Durango",
        "Instituto Green Hills de la Laguna":"Instituto Green Hills",
        "Colegio Israel III": "Colegio Israel 3",
        "Instituto Loyola de Chapala": "Loyola Asociación Cultura Educativa",
        "Instituto Loyola Unión":"Loyola Unión de Profesionistas y Empresarios",
        "Liceo Hispanoamericano ": "Liceo Hispanoamericano",
        "Colegio Premio Nobel": "Premio Nobel",
}
#CONEXION A LA BASE DE DATOS MVP1
#CONEXION A LA BASE DE DATOS MVP1
servidor = 'mattilda-prod.database.windows.net'
base_datos = 'Mattilda-Prod'
usuario = 'santiago.perez'
contraseña = 'Ps&6986kprJ3%9FXm'


# Establecer conexión
connection_mvp1 = pymssql.connect(server=servidor, user=usuario, password=contraseña, database=base_datos)
print('Conexión exitosa a la base de datos MVP1')



query_mvp1_COL = """
SELECT 
    IDColegio,
    Nombre AS nombre_colegio,
    IDConcepto,
    nombre_pf,
    email_pf,
    telefono_pf,
    IDAlumno,
    curp,
    nombre_completo,
    sep_2022,oct_2022,nov_2022,dic_2022,ene_2023,feb_2023,mar_2023,abr_2023,may_2023,jun_2023,jul_2023,ago_2023, sept_2023,oct_2023, nov_2023,dic_2023,ene_2024,
	sep_2022+oct_2022+nov_2022+dic_2022+ene_2023+feb_2023+mar_2023+abr_2023+may_2023+jun_2023+jul_2023+ago_2023+sept_2023+ oct_2023+ nov_2023 + dic_2023 + ene_2024 as Total,
	(case when sep_2022>0 then 1 else 0 end)+(case when oct_2022>0 then 1 else 0 end)+(case when nov_2022>0 then 1 else 0 end)+
	(case when dic_2022>0 then 1 else 0 end)+(case when ene_2023>0 then 1 else 0 end)+(case when feb_2023>0 then 1 else 0 end)+
	(case when mar_2023>0 then 1 else 0 end)+(case when abr_2023>0 then 1 else 0 end)+(case when may_2023>0 then 1 else 0 end)+
	(case when jun_2023>0 then 1 else 0 end)+(case when jul_2023>0 then 1 else 0 end)+(case when ago_2023>0 then 1 else 0 end)+
	(case when sept_2023>0 then 1 else 0 end)+ (case when oct_2023>0 then 1 else 0 end)+ (case when nov_2023 > 0 then 1 else 0 end)+
     (case when dic_2023>0 then 1 else 0 end) + (case when ene_2024 >0 then 1 else 0 end) N_Total
FROM (
    SELECT
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        MAX(CONCAT(tr.Nombre, ' ', tr.A_Paterno, ' ', tr.A_Materno)) AS nombre_pf,
        MAX(tr.Email) AS email_pf,
        MAX(tr.Telefono) AS telefono_pf,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno) AS Nombre_Completo,
        ta.CURP AS curp,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) <= 202209 then Saldo else 0 end)  sep_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202210 then Saldo else 0  end)  oct_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202211 then Saldo else 0  end)  nov_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202212 then Saldo else 0  end)  dic_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202301 then Saldo else 0  end)  ene_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202302 then Saldo else 0  end)  feb_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202303 then Saldo else 0  end)  mar_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202304 then Saldo else 0  end)  abr_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202305 then Saldo else 0  end)  may_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202306 then Saldo else 0  end)  jun_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202307 then Saldo else 0  end)  jul_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202308 then Saldo else 0  end)  ago_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202309 then Saldo else 0  end)  sept_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202310 then Saldo else 0  end)  oct_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202311 then Saldo else 0  end)  nov_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202312 then Saldo else 0  end)  dic_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202401 then Saldo else 0  end)  ene_2024
            
    FROM t_alumno ta
    LEFT JOIN t_cargos tc ON ta.IDAlumno = tc.IDAlumno
    JOIN cat_colegios cc ON ta.IDColegio = cc.IDColegio
    LEFT JOIN t_responsable tr ON tr.IDAlumno = ta.IDAlumno
    WHERE ta.IDEstatus = 'A'
	AND ta.Nombre not like '%Prueba%'
    GROUP BY 
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno),
        ta.CURP
) AS subquery
WHERE 
    IDColegio NOT IN (0,1,15,18,20,25,2,3,50,62, 63, 64,65,66,67,68,69,70, 54,39,59,60,  28) 
    AND IDAlumno NOT IN ('11743','22365','22582','22603','22611','22668','22809','22895','08639','08286','15158','20520', '18227','18427', '15934',
    '14914','14097','15158','12916','15168','025919','20520', '18227','18427', '14914','09712','21021','21604','23369','20935', '8415','19290','19205','23389','21447','20802','20913','20837','20747','20734','21673','20742','22730','22733','22436',
    '8415')  
    AND IDConcepto = 'COL'
"""

query_mvp1_REC = """
SELECT 
    IDColegio,
    Nombre AS nombre_colegio,
    IDConcepto,
    nombre_pf,
    email_pf,
    telefono_pf,
    IDAlumno,
    curp,
    nombre_completo,
    sep_2022,oct_2022,nov_2022,dic_2022,ene_2023,feb_2023,mar_2023,abr_2023,may_2023,jun_2023,jul_2023,ago_2023, sept_2023,oct_2023, nov_2023,dic_2023,ene_2024,
	sep_2022+oct_2022+nov_2022+dic_2022+ene_2023+feb_2023+mar_2023+abr_2023+may_2023+jun_2023+jul_2023+ago_2023+sept_2023+ oct_2023+ nov_2023 + dic_2023 + ene_2024 as Total,
	(case when sep_2022>0 then 1 else 0 end)+(case when oct_2022>0 then 1 else 0 end)+(case when nov_2022>0 then 1 else 0 end)+
	(case when dic_2022>0 then 1 else 0 end)+(case when ene_2023>0 then 1 else 0 end)+(case when feb_2023>0 then 1 else 0 end)+
	(case when mar_2023>0 then 1 else 0 end)+(case when abr_2023>0 then 1 else 0 end)+(case when may_2023>0 then 1 else 0 end)+
	(case when jun_2023>0 then 1 else 0 end)+(case when jul_2023>0 then 1 else 0 end)+(case when ago_2023>0 then 1 else 0 end)+
	(case when sept_2023>0 then 1 else 0 end)+ (case when oct_2023>0 then 1 else 0 end) + (case when nov_2023 > 0 then 1 else 0 end) 
    +(case when dic_2023 > 0 then 1 else 0 end)+ (case when ene_2024>0 then 1 else 0 end) N_Total
FROM (
    SELECT
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        MAX(CONCAT(tr.Nombre, ' ', tr.A_Paterno, ' ', tr.A_Materno)) AS nombre_pf,
        MAX(tr.Email) AS email_pf,
        MAX(tr.Telefono) AS telefono_pf,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno) AS Nombre_Completo,
        ta.CURP AS curp,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) <= 202209 then Saldo else 0 end)  sep_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202210 then Saldo else 0  end)  oct_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202211 then Saldo else 0  end)  nov_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202212 then Saldo else 0  end)  dic_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202301 then Saldo else 0  end)  ene_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202302 then Saldo else 0  end)  feb_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202303 then Saldo else 0  end)  mar_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202304 then Saldo else 0  end)  abr_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202305 then Saldo else 0  end)  may_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202306 then Saldo else 0  end)  jun_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202307 then Saldo else 0  end)  jul_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202308 then Saldo else 0  end)  ago_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202309 then Saldo else 0  end)  sept_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202310 then Saldo else 0  end)  oct_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202311 then Saldo else 0  end)  nov_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202312 then Saldo else 0  end)  dic_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202401 then Saldo else 0  end)  ene_2024
    FROM t_alumno ta
    LEFT JOIN t_cargos tc ON ta.IDAlumno = tc.IDAlumno
    JOIN cat_colegios cc ON ta.IDColegio = cc.IDColegio
    LEFT JOIN t_responsable tr ON tr.IDAlumno = ta.IDAlumno
    WHERE ta.IDEstatus = 'A'
	AND ta.Nombre not like '%Prueba%'
    GROUP BY 
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno),
        ta.CURP
) AS subquery
WHERE 
    IDColegio NOT IN (0,1,15,18,20,25,2,3,50,62, 63, 64,65,66,67,68,69,70, 54,39,59,60,28) 
    AND IDAlumno NOT IN ('11743','22365','22582','22603','22611','22668','22809','22895','08639','08286','15158','20520', '18227','18427', '15934',
    '14914','14097','15158','12916','15168','025919','20520', '18227','18427', '14914','09712','21021','21604','23369','20935', '8415','19290','19205','23389','21447','20802','20913','20837','20747','20734','21673','20742','22730','22733','22436',
    '8415')
    AND IDConcepto = 'REC'

"""


df_deudores_1_COL = pd.read_sql(query_mvp1_COL, connection_mvp1)
df_deudores_1_REC = pd.read_sql(query_mvp1_REC, connection_mvp1)


connection_mvp1.close()




print(df_deudores_1_COL)
print(df_deudores_1_COL['IDConcepto'].unique())
print(df_deudores_1_REC)
print(df_deudores_1_REC['IDConcepto'].unique())
print(df_deudores_1_REC['IDConcepto'].value_counts())



#CONEXION A LA BASE DE DATOS INDO
servidor = 'mattilda-prod.database.windows.net'
base_datos = 'Mattilda-Prod-Indo'
usuario = 'santiago.perez'
contraseña = 'Ps&6986kprJ3%9FXm'


# Establecer conexión
connection_mvp1_INDO = pymssql.connect(server=servidor, user=usuario, password=contraseña, database=base_datos)
print('Conexión exitosa a la base de datos INDO')


query_mvp1_COL_INDO = """
SELECT
    IDColegio,
    Nombre AS nombre_colegio,
    IDConcepto,
    nombre_pf,
    email_pf,
    telefono_pf,
    IDAlumno,
    curp,
    nombre_completo,
    sep_2022,oct_2022,nov_2022,dic_2022,ene_2023,feb_2023,mar_2023,abr_2023,may_2023,jun_2023,jul_2023,ago_2023, sept_2023,oct_2023,nov_2023,dic_2023, ene_2024,
	sep_2022+oct_2022+nov_2022+dic_2022+ene_2023+feb_2023+mar_2023+abr_2023+may_2023+jun_2023+jul_2023+ago_2023+sept_2023+ oct_2023 + nov_2023 + dic_2023 + ene_2024 as Total,
	(case when sep_2022>0 then 1 else 0 end)+(case when oct_2022>0 then 1 else 0 end)+(case when nov_2022>0 then 1 else 0 end)+
	(case when dic_2022>0 then 1 else 0 end)+(case when ene_2023>0 then 1 else 0 end)+(case when feb_2023>0 then 1 else 0 end)+
	(case when mar_2023>0 then 1 else 0 end)+(case when abr_2023>0 then 1 else 0 end)+(case when may_2023>0 then 1 else 0 end)+
	(case when jun_2023>0 then 1 else 0 end)+(case when jul_2023>0 then 1 else 0 end)+(case when ago_2023>0 then 1 else 0 end)+
	(case when sept_2023>0 then 1 else 0 end)+ (case when oct_2023>0 then 1 else 0 end)  +(case when nov_2023>0 then 1 else 0 end)
	    + (case when dic_2023>0 then 1 else 0 end) + (case when ene_2024 > 0 then 1 else 0 end) Total
FROM (
    SELECT
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        MAX(CONCAT(tr.Nombre, ' ', tr.A_Paterno, ' ', tr.A_Materno)) AS nombre_pf,
        MAX(tr.Email) AS email_pf,
        MAX(tr.Telefono) AS telefono_pf,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno) AS Nombre_Completo,
        ta.CURP AS curp,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) <= 202209 then Saldo else 0 end)  sep_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202210 then Saldo else 0  end)  oct_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202211 then Saldo else 0  end)  nov_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202212 then Saldo else 0  end)  dic_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202301 then Saldo else 0  end)  ene_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202302 then Saldo else 0  end)  feb_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202303 then Saldo else 0  end)  mar_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202304 then Saldo else 0  end)  abr_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202305 then Saldo else 0  end)  may_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202306 then Saldo else 0  end)  jun_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202307 then Saldo else 0  end)  jul_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202308 then Saldo else 0  end)  ago_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202309 then Saldo else 0  end)  sept_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202310 then Saldo else 0  end)  oct_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202311 then Saldo else 0  end)  nov_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202312 then Saldo else 0  end)  dic_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202401 then Saldo else 0  end)  ene_2024

    FROM t_alumno ta
    LEFT JOIN t_cargos tc ON ta.IDAlumno = tc.IDAlumno
    JOIN cat_colegios cc ON ta.IDColegio = cc.IDColegio
    LEFT JOIN t_responsable tr ON tr.IDAlumno = ta.IDAlumno
    WHERE ta.IDEstatus = 'A'
	AND ta.Nombre not like '%Prueba%'
    GROUP BY
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno),
        ta.CURP
) AS subquery
WHERE
    IDConcepto = 'COL'

"""






df_deudores_1_COL_INDO = pd.read_sql(query_mvp1_COL_INDO, connection_mvp1_INDO)



connection_mvp1_INDO.close()

print(df_deudores_1_COL_INDO)
print(df_deudores_1_COL_INDO['IDConcepto'].unique())

#CONEXION A LA BASE DE DATOS UANE
servidor = 'mattilda-prod.database.windows.net'
base_datos = 'Mattilda-Prod-UANE'
usuario = 'santiago.perez'
contraseña = 'Ps&6986kprJ3%9FXm'


# Establecer conexión
connection_mvp1_UANE = pymssql.connect(server=servidor, user=usuario, password=contraseña, database=base_datos)
print('Conexión exitosa a la base de datos INDO')


query_mvp1_COL_UANE = """
SELECT
    IDColegio,
    Nombre AS nombre_colegio,
    IDConcepto,
    nombre_pf,
    email_pf,
    telefono_pf,
    IDAlumno,
    curp,
    nombre_completo,
    sep_2022,oct_2022,nov_2022,dic_2022,ene_2023,feb_2023,mar_2023,abr_2023,may_2023,jun_2023,jul_2023,ago_2023, sept_2023,oct_2023,nov_2023,dic_2023, ene_2024,
	sep_2022+oct_2022+nov_2022+dic_2022+ene_2023+feb_2023+mar_2023+abr_2023+may_2023+jun_2023+jul_2023+ago_2023+sept_2023+ oct_2023 + nov_2023 + dic_2023 + ene_2024 as Total,
	(case when sep_2022>0 then 1 else 0 end)+(case when oct_2022>0 then 1 else 0 end)+(case when nov_2022>0 then 1 else 0 end)+
	(case when dic_2022>0 then 1 else 0 end)+(case when ene_2023>0 then 1 else 0 end)+(case when feb_2023>0 then 1 else 0 end)+
	(case when mar_2023>0 then 1 else 0 end)+(case when abr_2023>0 then 1 else 0 end)+(case when may_2023>0 then 1 else 0 end)+
	(case when jun_2023>0 then 1 else 0 end)+(case when jul_2023>0 then 1 else 0 end)+(case when ago_2023>0 then 1 else 0 end)+
	(case when sept_2023>0 then 1 else 0 end)+ (case when oct_2023>0 then 1 else 0 end)  +(case when nov_2023>0 then 1 else 0 end)
	    + (case when dic_2023>0 then 1 else 0 end) + (case when ene_2024 > 0 then 1 else 0 end) Total
FROM (
    SELECT
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        MAX(CONCAT(tr.Nombre, ' ', tr.A_Paterno, ' ', tr.A_Materno)) AS nombre_pf,
        MAX(tr.Email) AS email_pf,
        MAX(tr.Telefono) AS telefono_pf,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno) AS Nombre_Completo,
        ta.CURP AS curp,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) <= 202209 then Saldo else 0 end)  sep_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202210 then Saldo else 0  end)  oct_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202211 then Saldo else 0  end)  nov_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202212 then Saldo else 0  end)  dic_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202301 then Saldo else 0  end)  ene_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202302 then Saldo else 0  end)  feb_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202303 then Saldo else 0  end)  mar_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202304 then Saldo else 0  end)  abr_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202305 then Saldo else 0  end)  may_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202306 then Saldo else 0  end)  jun_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202307 then Saldo else 0  end)  jul_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202308 then Saldo else 0  end)  ago_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202309 then Saldo else 0  end)  sept_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202310 then Saldo else 0  end)  oct_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202311 then Saldo else 0  end)  nov_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202312 then Saldo else 0  end)  dic_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202401 then Saldo else 0  end)  ene_2024

    FROM t_alumno ta
    LEFT JOIN t_cargos tc ON ta.IDAlumno = tc.IDAlumno
    JOIN cat_colegios cc ON ta.IDColegio = cc.IDColegio
    LEFT JOIN t_responsable tr ON tr.IDAlumno = ta.IDAlumno
    WHERE ta.IDEstatus = 'A'
	AND ta.Nombre not like '%Prueba%'
    GROUP BY
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno),
        ta.CURP
) AS subquery
WHERE
    IDConcepto = 'COL'

"""



df_deudores_1_COL_UANE = pd.read_sql(query_mvp1_COL_UANE, connection_mvp1_UANE)


connection_mvp1_UANE.close()

print(df_deudores_1_COL_UANE)
print(df_deudores_1_COL_UANE['IDConcepto'].unique())



#CONEXION A LA BASE DE DATOS ULA
servidor = 'mattilda-prod.database.windows.net'
base_datos = 'Mattilda-Prod-ULA'
usuario = 'santiago.perez'
contraseña = 'Ps&6986kprJ3%9FXm'

# Establecer conexión
connection_mvp1_ULA = pymssql.connect(server=servidor, user=usuario, password=contraseña, database=base_datos)
print('Conexión exitosa a la base de datos INDO')


query_mvp1_COL_ULA = """
SELECT
    IDColegio,
    Nombre AS nombre_colegio,
    IDConcepto,
    nombre_pf,
    email_pf,
    telefono_pf,
    IDAlumno,
    curp,
    nombre_completo,
    sep_2022,oct_2022,nov_2022,dic_2022,ene_2023,feb_2023,mar_2023,abr_2023,may_2023,jun_2023,jul_2023,ago_2023, sept_2023,oct_2023,nov_2023,dic_2023, ene_2024,
	sep_2022+oct_2022+nov_2022+dic_2022+ene_2023+feb_2023+mar_2023+abr_2023+may_2023+jun_2023+jul_2023+ago_2023+sept_2023+ oct_2023 + nov_2023 + dic_2023 + ene_2024 as Total,
	(case when sep_2022>0 then 1 else 0 end)+(case when oct_2022>0 then 1 else 0 end)+(case when nov_2022>0 then 1 else 0 end)+
	(case when dic_2022>0 then 1 else 0 end)+(case when ene_2023>0 then 1 else 0 end)+(case when feb_2023>0 then 1 else 0 end)+
	(case when mar_2023>0 then 1 else 0 end)+(case when abr_2023>0 then 1 else 0 end)+(case when may_2023>0 then 1 else 0 end)+
	(case when jun_2023>0 then 1 else 0 end)+(case when jul_2023>0 then 1 else 0 end)+(case when ago_2023>0 then 1 else 0 end)+
	(case when sept_2023>0 then 1 else 0 end)+ (case when oct_2023>0 then 1 else 0 end)  +(case when nov_2023>0 then 1 else 0 end)
	    + (case when dic_2023>0 then 1 else 0 end) + (case when ene_2024 > 0 then 1 else 0 end) Total
FROM (
    SELECT
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        MAX(CONCAT(tr.Nombre, ' ', tr.A_Paterno, ' ', tr.A_Materno)) AS nombre_pf,
        MAX(tr.Email) AS email_pf,
        MAX(tr.Telefono) AS telefono_pf,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno) AS Nombre_Completo,
        ta.CURP AS curp,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) <= 202209 then Saldo else 0 end)  sep_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202210 then Saldo else 0  end)  oct_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202211 then Saldo else 0  end)  nov_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202212 then Saldo else 0  end)  dic_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202301 then Saldo else 0  end)  ene_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202302 then Saldo else 0  end)  feb_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202303 then Saldo else 0  end)  mar_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202304 then Saldo else 0  end)  abr_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202305 then Saldo else 0  end)  may_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202306 then Saldo else 0  end)  jun_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202307 then Saldo else 0  end)  jul_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202308 then Saldo else 0  end)  ago_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202309 then Saldo else 0  end)  sept_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202310 then Saldo else 0  end)  oct_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202311 then Saldo else 0  end)  nov_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202312 then Saldo else 0  end)  dic_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202401 then Saldo else 0  end)  ene_2024

    FROM t_alumno ta
    LEFT JOIN t_cargos tc ON ta.IDAlumno = tc.IDAlumno
    JOIN cat_colegios cc ON ta.IDColegio = cc.IDColegio
    LEFT JOIN t_responsable tr ON tr.IDAlumno = ta.IDAlumno
    WHERE ta.IDEstatus = 'A'
	AND ta.Nombre not like '%Prueba%'
    GROUP BY
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno),
        ta.CURP
) AS subquery
WHERE
    IDConcepto = 'COL'
"""




df_deudores_1_COL_ULA = pd.read_sql(query_mvp1_COL_ULA, connection_mvp1_ULA)


connection_mvp1_ULA.close()

print(df_deudores_1_COL_ULA)
print(df_deudores_1_COL_ULA['IDConcepto'].unique())



#CONEXION A LA BASE DE DATOS UTC
servidor = 'mattilda-prod.database.windows.net'
base_datos = 'Mattilda-Prod-UTC'
usuario = 'santiago.perez'
contraseña = 'Ps&6986kprJ3%9FXm'


# Establecer conexión
connection_mvp1_UTC = pymssql.connect(server=servidor, user=usuario, password=contraseña, database=base_datos)
print('Conexión exitosa a la base de datos INDO')


query_mvp1_COL_UTC = """
SELECT
    IDColegio,
    Nombre AS nombre_colegio,
    IDConcepto,
    nombre_pf,
    email_pf,
    telefono_pf,
    IDAlumno,
    curp,
    nombre_completo,
    sep_2022,oct_2022,nov_2022,dic_2022,ene_2023,feb_2023,mar_2023,abr_2023,may_2023,jun_2023,jul_2023,ago_2023, sept_2023,oct_2023,nov_2023,dic_2023, ene_2024,
	sep_2022+oct_2022+nov_2022+dic_2022+ene_2023+feb_2023+mar_2023+abr_2023+may_2023+jun_2023+jul_2023+ago_2023+sept_2023+ oct_2023 + nov_2023 + dic_2023 + ene_2024 as Total,
	(case when sep_2022>0 then 1 else 0 end)+(case when oct_2022>0 then 1 else 0 end)+(case when nov_2022>0 then 1 else 0 end)+
	(case when dic_2022>0 then 1 else 0 end)+(case when ene_2023>0 then 1 else 0 end)+(case when feb_2023>0 then 1 else 0 end)+
	(case when mar_2023>0 then 1 else 0 end)+(case when abr_2023>0 then 1 else 0 end)+(case when may_2023>0 then 1 else 0 end)+
	(case when jun_2023>0 then 1 else 0 end)+(case when jul_2023>0 then 1 else 0 end)+(case when ago_2023>0 then 1 else 0 end)+
	(case when sept_2023>0 then 1 else 0 end)+ (case when oct_2023>0 then 1 else 0 end)  +(case when nov_2023>0 then 1 else 0 end)
	    + (case when dic_2023>0 then 1 else 0 end) + (case when ene_2024 > 0 then 1 else 0 end) Total
FROM (
    SELECT
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        MAX(CONCAT(tr.Nombre, ' ', tr.A_Paterno, ' ', tr.A_Materno)) AS nombre_pf,
        MAX(tr.Email) AS email_pf,
        MAX(tr.Telefono) AS telefono_pf,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno) AS Nombre_Completo,
        ta.CURP AS curp,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) <= 202209 then Saldo else 0 end)  sep_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202210 then Saldo else 0  end)  oct_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202211 then Saldo else 0  end)  nov_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202212 then Saldo else 0  end)  dic_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202301 then Saldo else 0  end)  ene_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202302 then Saldo else 0  end)  feb_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202303 then Saldo else 0  end)  mar_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202304 then Saldo else 0  end)  abr_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202305 then Saldo else 0  end)  may_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202306 then Saldo else 0  end)  jun_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202307 then Saldo else 0  end)  jul_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202308 then Saldo else 0  end)  ago_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202309 then Saldo else 0  end)  sept_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202310 then Saldo else 0  end)  oct_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202311 then Saldo else 0  end)  nov_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202312 then Saldo else 0  end)  dic_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202401 then Saldo else 0  end)  ene_2024

    FROM t_alumno ta
    LEFT JOIN t_cargos tc ON ta.IDAlumno = tc.IDAlumno
    JOIN cat_colegios cc ON ta.IDColegio = cc.IDColegio
    LEFT JOIN t_responsable tr ON tr.IDAlumno = ta.IDAlumno
    WHERE ta.IDEstatus = 'A'
	AND ta.Nombre not like '%Prueba%'
    GROUP BY
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno),
        ta.CURP
) AS subquery
WHERE
    IDConcepto = 'COL'

"""



df_deudores_1_COL_UTC = pd.read_sql(query_mvp1_COL_UTC, connection_mvp1_UTC)


connection_mvp1_UTC.close()

print(df_deudores_1_COL_UTC)
print(df_deudores_1_COL_UTC['IDConcepto'].unique())






#CONEXION A LA BASE DE DATOS UTEG
servidor = 'mattilda-prod.database.windows.net'
base_datos = 'Mattilda-Prod-UTEG'
usuario = 'santiago.perez'
contraseña = 'Ps&6986kprJ3%9FXm'


# Establecer conexión
connection_mvp1_UTEG = pymssql.connect(server=servidor, user=usuario, password=contraseña, database=base_datos)
print('Conexión exitosa a la base de datos INDO')


query_mvp1_COL_UTEG = """
SELECT
    IDColegio,
    Nombre AS nombre_colegio,
    IDConcepto,
    nombre_pf,
    email_pf,
    telefono_pf,
    IDAlumno,
    curp,
    nombre_completo,
    sep_2022,oct_2022,nov_2022,dic_2022,ene_2023,feb_2023,mar_2023,abr_2023,may_2023,jun_2023,jul_2023,ago_2023, sept_2023,oct_2023,nov_2023,dic_2023, ene_2024,
	sep_2022+oct_2022+nov_2022+dic_2022+ene_2023+feb_2023+mar_2023+abr_2023+may_2023+jun_2023+jul_2023+ago_2023+sept_2023+ oct_2023 + nov_2023 + dic_2023 + ene_2024 as Total,
	(case when sep_2022>0 then 1 else 0 end)+(case when oct_2022>0 then 1 else 0 end)+(case when nov_2022>0 then 1 else 0 end)+
	(case when dic_2022>0 then 1 else 0 end)+(case when ene_2023>0 then 1 else 0 end)+(case when feb_2023>0 then 1 else 0 end)+
	(case when mar_2023>0 then 1 else 0 end)+(case when abr_2023>0 then 1 else 0 end)+(case when may_2023>0 then 1 else 0 end)+
	(case when jun_2023>0 then 1 else 0 end)+(case when jul_2023>0 then 1 else 0 end)+(case when ago_2023>0 then 1 else 0 end)+
	(case when sept_2023>0 then 1 else 0 end)+ (case when oct_2023>0 then 1 else 0 end)  +(case when nov_2023>0 then 1 else 0 end)
	    + (case when dic_2023>0 then 1 else 0 end) + (case when ene_2024 > 0 then 1 else 0 end) Total
FROM (
    SELECT
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        MAX(CONCAT(tr.Nombre, ' ', tr.A_Paterno, ' ', tr.A_Materno)) AS nombre_pf,
        MAX(tr.Email) AS email_pf,
        MAX(tr.Telefono) AS telefono_pf,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno) AS Nombre_Completo,
        ta.CURP AS curp,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) <= 202209 then Saldo else 0 end)  sep_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202210 then Saldo else 0  end)  oct_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202211 then Saldo else 0  end)  nov_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202212 then Saldo else 0  end)  dic_2022,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202301 then Saldo else 0  end)  ene_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202302 then Saldo else 0  end)  feb_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202303 then Saldo else 0  end)  mar_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202304 then Saldo else 0  end)  abr_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202305 then Saldo else 0  end)  may_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202306 then Saldo else 0  end)  jun_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202307 then Saldo else 0  end)  jul_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202308 then Saldo else 0  end)  ago_2023,
			sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202309 then Saldo else 0  end)  sept_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202310 then Saldo else 0  end)  oct_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202311 then Saldo else 0  end)  nov_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202312 then Saldo else 0  end)  dic_2023,
            sum(case when YEAR(Fecha_Vencimiento)*100 + MONTH(Fecha_Vencimiento) = 202401 then Saldo else 0  end)  ene_2024

    FROM t_alumno ta
    LEFT JOIN t_cargos tc ON ta.IDAlumno = tc.IDAlumno
    JOIN cat_colegios cc ON ta.IDColegio = cc.IDColegio
    LEFT JOIN t_responsable tr ON tr.IDAlumno = ta.IDAlumno
    WHERE ta.IDEstatus = 'A'
	AND ta.Nombre not like '%Prueba%'
    GROUP BY
        ta.IDColegio,
        cc.Nombre,
        IDConcepto,
        tc.idalumno,
        CONCAT(ta.Nombre, ' ', ta.A_Paterno, ' ', ta.A_Materno),
        ta.CURP
) AS subquery
WHERE
    IDConcepto = 'COL'

"""


df_deudores_1_COL_UTEG = pd.read_sql(query_mvp1_COL_UTEG, connection_mvp1_UTEG)


connection_mvp1_UTEG.close()

print(df_deudores_1_COL_UTEG)
print(df_deudores_1_COL_UTEG['IDConcepto'].unique())





#CONEXION A LA BASE DE DATOS MVP2
conn_mvp2 = psycopg2.connect(
        dbname="MattiProductionDb",
        user="postgres",
        password="5DZPewBfrmW6CF6c",
        host="matti-production-aurora-cluster.cluster-cu0j0dopeo4q.us-east-1.rds.amazonaws.com",
        port="5432"
    )
cursor = conn_mvp2.cursor()
print('Conexión exitosa a la base de datos MVP2')


query_mvp2_COL= """
WITH monthly_sums AS (
    SELECT
        i.student_id,
        EXTRACT(YEAR FROM i.due_date) AS year,
        EXTRACT(MONTH FROM i.due_date) AS month,
        SUM(i.pending_amount) AS monto
    FROM
        invoices i
    WHERE
        (i.due_date BETWEEN '2022-09-01' AND '2024-01-31')
    AND i.status in( 'expired')
    GROUP BY
        i.student_id,
        EXTRACT(YEAR FROM i.due_date),
        EXTRACT(MONTH FROM i.due_date)
)

SELECT
    c.id AS IDColegio,
    c.name AS nombre_Colegio,
    'membership' AS IDConcepto,
    min(u.full_name) as nombre_PF,
    MIN(u.email) as email_PF,
    MIN(u.phone) as telefono_PF,
    s.matti_id AS IDAlumno,
    s.curp as curp,
    s.full_name AS Nombre_Completo,

    -- Pivot the monthly sums
    COALESCE(MAX(CASE WHEN year = 2022 AND month = 9 THEN monto END), 0) AS sept_2022,
    COALESCE(MAX(CASE WHEN year = 2022 AND month = 10 THEN monto END), 0) AS oct_2022,
    COALESCE(MAX(CASE WHEN year = 2022 AND month = 11 THEN monto END), 0) AS nov_2022,
    COALESCE(MAX(CASE WHEN year = 2022 AND month = 12 THEN monto END), 0) AS dic_2022,
    COALESCE(MAX(CASE WHEN year = 2023 AND month = 1 THEN monto END), 0) AS ene_2023,
    COALESCE(MAX(CASE WHEN year = 2023 AND month = 2 THEN monto END), 0) AS feb_2023,
    COALESCE(MAX(CASE WHEN year = 2023 AND month = 3 THEN monto END), 0) AS mar_2023,
    COALESCE(MAX(CASE WHEN year = 2023 AND month = 4 THEN monto END), 0) AS abr_2023,
    COALESCE(MAX(CASE WHEN year = 2023 AND month = 5 THEN monto END), 0) AS may_2023,
    COALESCE(MAX(CASE WHEN year = 2023 AND month = 6 THEN monto END), 0) AS jun_2023,
    COALESCE(MAX(CASE WHEN year = 2023 AND month = 7 THEN monto END), 0) AS jul_2023,
    COALESCE(MAX(CASE WHEN year = 2023 AND month = 8 THEN monto END), 0) AS ago_2023,
    COALESCE(MAX(CASE WHEN year = 2023 AND month = 9 THEN monto END), 0) AS sept_2023,
    COALESCE(MAX(CASE WHEN year = 2023 AND month = 10 THEN monto END), 0) AS oct_2023,
    COALESCE(MAX(CASE WHEN year = 2023 AND month = 11 THEN monto END), 0) AS nov_2023,
    COALESCE(MAX(CASE WHEN year = 2023 AND month = 12 THEN monto END), 0) AS dic_2023,
    COALESCE(MAX(CASE WHEN year = 2024 AND month = 1 THEN monto END), 0) AS ene_2024,

    CASE WHEN SUM(monto) is NULL  THEN 0
        ELSE SUM(monto)
        END AS total_monto,

        SUM(CASE WHEN monto > 0 THEN 1 ELSE 0 END) AS N_Total
FROM
    students s
JOIN
    campus_students cs ON s.id = cs.student_id
JOIN
     campuses c on cs.campus_id = c.id
LEFT JOIN
    monthly_sums ON s.id = monthly_sums.student_id
LEFT JOIN family_groups_users fgu on s.id = fgu.student_id
LEFT JOIN users u on fgu.parent_id = u.id
WHERE c.id not in ('3b3b63b8-2253-49d8-b3ea-20d88c2fa8bb','8600956f-2fb8-413d-b703-bd5b612abfca', 'b2eb68b0-79b5-4681-b20d-fa26bfdaa65b',
'44fe89d3-4da9-4e8e-9549-cf243d61c857','5fabb810-3872-463d-a1d2-9bfcba0ecf87','17b3a3c0-243c-4d6c-9108-351b529b2266','eef06116-2516-44e1-a339-0311bd3f79b8','eef06116-2516-44e1-a339-0311bd3f79b8')
AND s.full_name not like '%Factura%'
AND cs.status = 'active'
AND u.referred_id is null
AND s.matti_id  not in ('301997236', '514656523')
GROUP BY
    c.id, c.name, s.matti_id, s.full_name, s.curp
"""


df_deudores_2_COL = pd.read_sql(query_mvp2_COL, conn_mvp2)


conn_mvp2.close()
print(df_deudores_2_COL)
df_deudores_2_COL.to_csv('df_deudores_2_COL.csv', index=False)


df_deudores_2_COL = df_deudores_2_COL.rename(columns={
    'idcolegio': 'IDColegio',
    'nombre_colegio': 'nombre_colegio',
    'idconcepto': 'IDConcepto',
    'nombre_pf': 'nombre_pf',
    'email_pf': 'email_pf',
    'telefono_pf': 'telefono_pf',
    'idalumno': 'IDAlumno',
    'curp': 'curp',
    'nombre_completo': 'nombre_completo',
    'sept_2022': 'sep_2022',
    'oct_2022': 'oct_2022',
    'nov_2022': 'nov_2022',
    'dic_2022': 'dic_2022',
    'ene_2023': 'ene_2023',
    'feb_2023': 'feb_2023',
    'mar_2023': 'mar_2023',
    'abr_2023': 'abr_2023',
    'may_2023': 'may_2023',
    'jun_2023': 'jun_2023',
    'jul_2023': 'jul_2023',
    'ago_2023': 'ago_2023',
    'sept_2023': 'sept_2023',
    'oct_2023': 'oct_2023',
    'nov_2023':'nov_2023',
    'dic_2023': 'dic_2023',
    'ene_2024': 'ene_2024',
    'total_monto': 'Total',
    'n_total': 'N_Total'

    
})



#CONEXION A LA BASE DE DATOS MATTIHOUSE

dbname="mattihouse"
user="postgres"
password="F|9T*m|hvZYDgt>pQ*LmO}N6R7dd"
host="mattihouse.cluster-ro-cu0j0dopeo4q.us-east-1.rds.amazonaws.com"
port="5432"


# Establecer conexión con PostgreSQL
conexion_str_mattihouse = f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}"
engine = sqlalchemy.create_engine(conexion_str_mattihouse)
print('Conexión exitosa a la base de datos MattiHouse')

with engine.begin() as connection:
    result = connection.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'mattildaprod'
            AND table_name = 'Deudores_COL'
        );
    """))
    table_exists = result.scalar()

if table_exists:
    print("La tabla Deudores_COL existe.")
else:
    print("La tabla Deudores_COL no existe.")


# Truncar las tablas
with engine.begin() as connection:
   connection.execute(text('TRUNCATE TABLE mattildaprod."Deudores_COL" RESTART IDENTITY CASCADE;'))
   connection.execute(text('TRUNCATE TABLE mattildaprod."Deudores_REC" RESTART IDENTITY CASCADE;'))



print("Tablas truncadas.")

unificar_IDColegio = { 
    "Colegio Mater Dei":  "25a7e77f-63df-4209-81b7-b42a6f7dae70",
    "SEBEC Preescolar": "60085e9e-41e3-4402-8cb5-6feabb86e9a1",
    "SEBEC Preparatoria": "28488020-a0a5-43b5-b384-268e724880e4",
    "SEBEC Primaria": "41a50b9d-6db1-40eb-a650-a4e27d9aec85",
    "SEBEC Secundaria": "bd272506-ef18-4e4e-9b3d-21a7f2b7ed95",
    "Centro de Estudios Patzcuarense": "6481a817-68e2-4687-b996-11427d9fb092",
    "Centro Educativo Alianza" : "edfab51c-bb1d-4521-b143-c8fe1be96610",
    "Colegio Asbaje": "d57defc0-fecb-4fbf-84d8-9b5c65a73027",
    "Colegio Israel": "d40ec3d3-9dca-41a6-b524-fec9fad57700",
    "Colegio Miramar": "527dd122-aab1-4972-ae27-0ddcc4bad34b",
    "Instituto Anglia": "11fea0ab-266f-4fe4-8537-4498b423a5ca",
    "Instituto Green Hills de la Laguna": "9027ad39-8bf7-4e82-b3fd-0877d62f1e61",
    "Instituto Liberatec": "907c658e-62d6-4168-9ae8-a223969ba845",
    "Instituto México Inglés": "2c016e77-dbc6-4d50-b14e-f5714a80e873",
    "Instituto Tepeyac Toluca": "5c347638-b762-40b4-ac00-0ddeeb94a2f1",
    "Univer Durango": "49cd42de-e438-439e-83c7-ca512a0c50f1",
    "Colegio Romera": "ac5cc5d2-c023-47c0-af96-dcc05c9087a6",
    "Colegio Miramar": "527dd122-aab1-4972-ae27-0ddcc4bad34b",
    "Univer Durango" : "49cd42de-e438-439e-83c7-ca512a0c50f1",
    "Universidad UNIVER Bachillerato": "e9993b8d-1d43-4542-857b-a48946f52113",
    "Universidad UNIVER de Nayarit": "15c88ec8-0bac-471f-9eb0-9e9039eab076",
    "Centro de Estudios Patzcuarense":"9e244fc4-be83-43b2-8c8d-1edb3adf0622",
    "Colegio Miguel Hidalgo": "c780346f-3e50-452c-87e4-56b3497727622",
    "Universidad Tec de Oriente": "a974bdb9-bac8-41f1-865e-391411dae6d6",
    "Colegio Jean Piaget": "aeade7c2-8ab0-40cc-b9ea-caaa2db410d9",
  
}

# Función que regresa el ID actualizado si el colegio está en el diccionario, 
# o el ID original si no está en el diccionario
def unificar_ids(row):
    return unificar_IDColegio.get(row['nombre_colegio'], row['IDColegio'])

# Aplicamos la función usando el método 'apply' del DataFramdf_deudores_1_COL['IDColegio'] = df_deudores_1_COL.apply(unificar_ids, axis=1)

print(df_deudores_1_COL.columns)


df_merge = df_deudores_1_COL.merge(df_deudores_1_REC, on='IDAlumno', how='outer', suffixes=('_COL', '_REC'))


# Columnas que queremos sumar
columns_to_sum = ['sep_2022', 'oct_2022', 'nov_2022', 'dic_2022', 'ene_2023', 
                  'feb_2023', 'mar_2023', 'abr_2023', 'may_2023', 'jun_2023', 
                  'jul_2023', 'ago_2023', 'sept_2023','oct_2023' ,'nov_2023','dic_2023','ene_2024','Total', 'N_Total']


# Sumar las columnas de montos y reemplazar NaN por 0
for month in columns_to_sum:  # Puedes agregar otros meses aquí
    df_merge[month] = df_merge[month + '_COL'].fillna(0) + df_merge[month + '_REC'].fillna(0)
    # Eliminar columnas redundantes
    df_merge.drop([month + '_COL', month + '_REC'], axis=1, inplace=True)

# Asegurarse de que la columna 'NombreAlumno' no sea redundante
df_merge['nombre_completo'] = df_merge['nombre_completo_COL'].where(df_merge['nombre_completo_COL'].notna(), df_merge['nombre_completo_REC'])
df_merge.drop(['nombre_completo_COL', 'nombre_completo_REC'], axis=1, inplace=True)
# Renombrar la columna
df_merge = df_merge.rename(columns={'IDColegio_COL': 'IDColegio'})
df_merge = df_merge.rename(columns={'nombre_colegio_COL': 'nombre_colegio'})
df_merge = df_merge.rename(columns={'IDConcepto_COL': 'IDConcepto'})
df_merge = df_merge.rename(columns={'curp_COL': 'curp'})
df_merge = df_merge.rename(columns={'nombre_pf_COL': 'nombre_pf'})
df_merge = df_merge.rename(columns={'email_pf_COL': 'email_pf'})
df_merge = df_merge.rename(columns={'telefono_pf_COL': 'telefono_pf'})
df_merge = df_merge.rename(columns={'IDAlumno_COL': 'IDAlumno'})


# Ahora, para reordenar las columnas según la lista que proporcionaste anteriormente:
column_order = [
    'IDColegio', 'nombre_colegio', 'IDConcepto','nombre_pf','email_pf', 'telefono_pf' ,'IDAlumno', 'curp' ,'nombre_completo', 
    'sep_2022', 'oct_2022', 'nov_2022', 'dic_2022', 'ene_2023', 
    'feb_2023', 'mar_2023', 'abr_2023', 'may_2023', 'jun_2023', 
    'jul_2023', 'ago_2023', 'sept_2023','oct_2023' , 'nov_2023','dic_2023','ene_2024','Total', 'N_Total'
]


print(df_merge)

print(df_merge)

# Guardar el DataFrame en un archivo CSV
df_merge.to_csv('deudores.csv', index=False)
print("dataframe guardado en csv")


# 1. Concatenar df_deudores_2_COL y df_merge
df_concatenated = pd.concat([df_deudores_2_COL, df_merge], ignore_index=True)

# 2. No necesitamos un paso explícito de ordenación porque, al concatenar, las filas de df_deudores_2_COL ya están al principio.

# 3. Eliminar duplicados

#df_concatenated.to_csv('deudores_final1.csv', index=False)
df_final = df_concatenated.drop_duplicates(subset='IDAlumno', keep='first')
df_final=df_concatenated.drop_duplicates(subset='curp', keep='first')

print(df_final)
df_final.to_csv('deudores_final.csv', index=False)

df_final.loc[:, 'nombre_colegio'] = df_final['nombre_colegio'].replace(nombres_colegio)
with engine.begin() as connection:
    connection.execute(text('TRUNCATE TABLE mattildaprod."Deudores_COL" RESTART IDENTITY CASCADE;'))
print("Tablas truncadas.")

with engine.begin() as connection:
    connection.execute(text('TRUNCATE TABLE mattildaprod."Deudores_REC" RESTART IDENTITY CASCADE;'))
print("Tablas truncadas.")


#merged_df.to_sql('Deudores_COL', engine, schema='mattildaprod', if_exists='append', index=False)
df_final.to_sql('Deudores_COL', engine, schema='mattildaprod', if_exists='replace', index=False)
print("COL LISTAS")
df_deudores_1_COL_INDO.to_sql('Deudores_COL', engine, schema='mattildaprod', if_exists='append', index=False)
df_deudores_1_COL_UTC.to_sql('Deudores_COL', engine, schema='mattildaprod', if_exists='append', index=False)
df_deudores_1_COL_UTEG.to_sql('Deudores_COL', engine, schema='mattildaprod', if_exists='append', index=False)
df_deudores_1_COL_ULA.to_sql('Deudores_COL', engine, schema='mattildaprod', if_exists='append', index=False)
df_deudores_1_COL_UANE.to_sql('Deudores_COL', engine, schema='mattildaprod', if_exists='append', index=False)
print("COL LISTAS")

#df_deudores_1_REC.to_sql('Deudores_REC', engine, schema='mattildaprod', if_exists='replace', index=False)
print("REC1 LISTAS")




print("DataFrames guardados en la base de datos MattiHouse.")

query_mattihouse_union = """

SELECT
    col."IDColegio",
    col.nombre_colegio,
    col."IDConcepto",
    col.nombre_pf,
    col.email_pf,
    col.telefono_pf,
    col."IDAlumno",
    col.nombre_completo,

    -- Sumas para cada mes
    col.sep_2022 + COALESCE(rec.sep_2022, 0) AS sep_2022,
    col.oct_2022 + COALESCE(rec.oct_2022, 0) AS oct_2022,
    col.nov_2022 + COALESCE(rec.nov_2022, 0) AS nov_2022,
    col.dic_2022 + COALESCE(rec.dic_2022, 0) AS dic_2022,
    col.ene_2023 + COALESCE(rec.ene_2023, 0) AS ene_2023,
    col.feb_2023 + COALESCE(rec.feb_2023, 0) AS feb_2023,
    col.mar_2023 + COALESCE(rec.mar_2023, 0) AS mar_2023,
    col.abr_2023 + COALESCE(rec.abr_2023, 0) AS abr_2023,
    col.may_2023 + COALESCE(rec.may_2023, 0) AS may_2023,
    col.jun_2023 + COALESCE(rec.jun_2023, 0) AS jun_2023,
    col.jul_2023 + COALESCE(rec.jul_2023, 0) AS jul_2023,
    col.ago_2023 + COALESCE(rec.ago_2023, 0) AS ago_2023,
    col.sept_2023 + COALESCE(rec.sept_2023, 0) AS sept_2023,
    col.oct_2023 + COALESCE(rec.oct_2023, 0) AS oct_2023,
    col.nov_2023 + COALESCE(rec.nov_2023, 0) AS nov_2023,
    col.dic_2023 + COALESCE(rec.dic_2023, 0) AS dic_2023,
    col.ene_2024  AS ene_2024,


    -- Columna "Total" como suma de todos los meses
    (col.sep_2022 + col.oct_2022 + col.nov_2022 + col.dic_2022 + col.ene_2023 + col.feb_2023 + col.mar_2023 + col.abr_2023 + col.may_2023 + col.jun_2023 + col.jul_2023 + col.ago_2023+ col.sept_2023+ col.oct_2023+col.nov_2023+col.dic_2023+col.ene_2024)
    + COALESCE(rec.sep_2022, 0) + COALESCE(rec.oct_2022, 0) + COALESCE(rec.nov_2022, 0) + COALESCE(rec.dic_2022, 0) + COALESCE(rec.ene_2023, 0) + COALESCE(rec.feb_2023, 0)
    + COALESCE(rec.mar_2023, 0) + COALESCE(rec.abr_2023, 0) + COALESCE(rec.may_2023, 0) + COALESCE(rec.jun_2023, 0) + COALESCE(rec.jul_2023, 0) + COALESCE(rec.ago_2023, 0)
    + COALESCE(rec.sept_2023, 0)+ COALESCE(rec.oct_2023, 0)+ COALESCE(rec.nov_2023, 0)+ COALESCE(rec.dic_2023, 0) 
    AS "Total",

    -- Columna "N_Total" como cuenta de meses con monto > 0
    (CASE WHEN col.sep_2022 + COALESCE(rec.sep_2022, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.oct_2022 + COALESCE(rec.oct_2022, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.nov_2022 + COALESCE(rec.nov_2022, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.dic_2022 + COALESCE(rec.dic_2022, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.ene_2023 + COALESCE(rec.ene_2023, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.feb_2023 + COALESCE(rec.feb_2023, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.mar_2023 + COALESCE(rec.mar_2023, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.abr_2023 + COALESCE(rec.abr_2023, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.may_2023 + COALESCE(rec.may_2023, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.may_2023 + COALESCE(rec.may_2023, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.jun_2023 + COALESCE(rec.jun_2023, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.jul_2023 + COALESCE(rec.jul_2023, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.ago_2023 + COALESCE(rec.ago_2023, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.sept_2023 + COALESCE(rec.sept_2023, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.oct_2023 + COALESCE(rec.oct_2023, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.nov_2023 + COALESCE(rec.nov_2023, 0) > 0 THEN 1 ELSE 0 END)+
    (CASE WHEN col.dic_2023 + COALESCE(rec.dic_2023, 0) > 0 THEN 1 ELSE 0 END)
    + (CASE WHEN col.ene_2024  > 0 THEN 1 ELSE 0 END)
    AS "N_Total"

FROM
    mattildaprod."Deudores_COL" col
LEFT JOIN
    mattildaprod."Deudores_REC" rec
ON
    col."IDColegio" = rec."IDColegio"
    AND col."IDAlumno" = rec."IDAlumno"
    AND col."IDAlumno" = rec."IDAlumno";
"""




df_deudores_union = pd.read_sql(query_mattihouse_union, engine)
print(df_deudores_union)


# Truncar las tablas
with engine.begin() as connection:
    connection.execute(text('TRUNCATE TABLE mattildaprod."Deudores_COL_y_REC" RESTART IDENTITY CASCADE;'))
print("Tablas truncadas.")
df_deudores_union.to_sql('Deudores_COL_y_REC', engine, schema='mattildaprod', if_exists='replace', index=False)
print("Tablas actualizadas.")
fin  = time.time()
print("Proceso finalizado.")
print(f"Tiempo de ejecución: {fin - inicio} segundos.")



