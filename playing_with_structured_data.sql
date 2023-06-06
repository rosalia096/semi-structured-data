
--- Dato en crudo 

SELECT * FROM data_base.schema.orden_dato

----- Convertimos a lista de objetos 


CREATE OR REPLACE TEMPORARY TABLE gas.WORK.orden_dato_small AS 
SELECT 
order_id,
customer_id,
MAX(fecha_estimada_envio) AS fecha_estimada_envio,
array_agg(object_construct( 'tipo_operacion',tipo_operacion,'fecha_hora_operacion',fecha_hora_operacion)) WITHIN GROUP (ORDER BY fecha_hora_operacion ASC) AS eventos														
FROM data_base.schema.orden_dato
GROUP BY order_id,customer_id

----- Funciones 

CREATE OR REPLACE TEMPORARY FUNCTION data_base.schema.existe_tipo_operacion(EVENT ARRAY,VALUE STRING)
RETURNS BOOLEAN
LANGUAGE python
RUNTIME_VERSION = '3.8'
HANDLER = 'existe_tipo_operacion'
AS 
$$
def existe_tipo_operacion(event,value):
    l = any(x['tipo_operacion'] == value for x in event)
    return l
$$;


CREATE OR REPLACE TEMPORARY FUNCTION data_base.schema.posicion_anterior_posterior(EVENT ARRAY,VALUE STRING)
RETURNS ARRAY
LANGUAGE python
RUNTIME_VERSION = '3.8'
HANDLER = 'get_position'
AS 
$$
def get_position(event,operacion):
    operacion = [operacion]
    l1 = len(operacion)-1
    data = [d['tipo_operacion'] for d in event]
    position_min_s1 = [j for j in range(0, len(data) - len(operacion) + 1) if data[j: j + len(operacion)] == operacion]
    n1 = len(position_min_s1)
    if n1 > 0:
        position_max_s1 = list(map(lambda x: x + l1 , position_min_s1)) ; anterior = [event[position_min_s1[i] - 1] if position_min_s1[i] > 0 else None for i in range(n1)] ;  posterior = [event[position_max_s1[i]+1] if (position_max_s1[i]+1) < len(data) else None for i in range(n1)]
        return [anterior[0],posterior[0]]
    else:
        return None
$$;

---- LLamada a las funciones 

SELECT *,
data_base.schema.existe_tipo_operacion(eventos,'Enviada') AS compra_fue_enviada,
data_base.schema.posicion_anterior_posterior(eventos,'En proceso') AS evento_anterior_posterior
FROM data_base.schema.orden_dato_small


---- tiempo entre posicion anterior y posterior 

SELECT *, datediff(day, tiempo_anterior, tiempo_posterior) AS dias_entre_anterior_posterior
FROM (
	SELECT 
	* ,
	evento_anterior_posterior[0]['fecha_hora_operacion']::TIMESTAMP AS tiempo_anterior,
	evento_anterior_posterior[1]['fecha_hora_operacion']::TIMESTAMP AS tiempo_posterior
	FROM (
		SELECT *,
		data_base.schema.posicion_anterior_posterior(eventos,'En proceso') AS evento_anterior_posterior
		FROM data_base.schema.orden_dato_small
	)
	WHERE tiempo_anterior IS NOT NULL AND tiempo_posterior IS NOT NULL 
)


---- tiempo entre anterior y posterior mas "cool"

CREATE OR REPLACE TEMPORARY FUNCTION data_base.schema.tiempo_anterior_posterior(EVENT ARRAY,VALUE STRING)
RETURNS INT 
LANGUAGE python
RUNTIME_VERSION = '3.8'
HANDLER = 'get_position'
PACKAGES = ('pandas','numpy')
AS 
$$

import pandas
import numpy as np

def get_position(event,operacion):
    operacion = [operacion]
    l1 = len(operacion)-1
    data = [d['tipo_operacion'] for d in event]
    position_min_s1 = [j for j in range(0, len(data) - len(operacion) + 1) if data[j: j + len(operacion)] == operacion]
    n1 = len(position_min_s1)
    if n1 > 0:
        position_max_s1 = list(map(lambda x: x + l1 , position_min_s1));
        anterior = [event[position_min_s1[i] - 1]['fecha_hora_operacion'] if position_min_s1[i] > 0 else None for i in range(n1)] ;
        posterior = [event[position_max_s1[i]+1]['fecha_hora_operacion'] if (position_max_s1[i]+1) < len(data) else None for i in range(n1)]
        if anterior[0] is not None and posterior[0] is not None:
            return (pandas.to_datetime(posterior[0],format='%Y-%m-%d %H:%M:%S.%f') - pandas.to_datetime(anterior[0],format='%Y-%m-%d %H:%M:%S.%f')) / np.timedelta64(1, 'D')
    else:
        return None
$$;


SELECT *,
data_base.schema.tiempo_anterior_posterior(eventos,'En proceso') AS dias_entre_anterior_posterior
FROM data_base.schema.orden_dato_small 
WHERE dias_entre_anterior_posterior IS NOT NULL 

--------------------- Ejemplo de uso:
----- Resultados para responder a la necesidad : ¿Cual es la media de dias entre los eventos posteriores y anteriores al estatus 'En proceso'?

CREATE OR REPLACE TABLE data_base.schema.resultados_dato AS 
SELECT
o.order_id,
s.estatus_esp AS estatus ,
p.PRODUCT_CATEGORY_NAME AS categoria_producto,
data_base.schema.tiempo_anterior_posterior(eventos,'En proceso') AS dias_entre_anterior_posterior
FROM data_base.schema.orden_dato_small AS o
LEFT JOIN data_base.schema.orden_producto_dato AS op
ON o.order_id= op.order_id
LEFT JOIN data_base.schema.productos_dato AS p
ON op.product_id= p.product_id
LEFT JOIN data_base.schema.ordenes_status_dato AS s 
ON o.order_id= s.order_id
WHERE dias_entre_anterior_posterior IS NOT NULL 
