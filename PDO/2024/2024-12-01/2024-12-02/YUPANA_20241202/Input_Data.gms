*****************************************
*** DATOS DE ENTRADA AL MODELO YUPANA ***
*****************************************

$OnMultiR
$OnEmpty

$IFTHEN exist Periodos.dat
$   INCLUDE Periodos.dat
$ENDIF

$IFTHEN exist Sets.inc
$   INCLUDE Sets.inc
$   CALL grep GAS PlantaTermica.txt > Termicas_a_Gas.txt
    SETS Ut_Gas(Ut) /
$       INCLUDE Termicas_a_Gas.txt
    /;
$ENDIF

$IFTHEN exist Configuraciones.dat
$   INCLUDE Configuraciones.dat
$ENDIF

$IFTHEN exist Metodo_de_Ejecucion.dat
$   INCLUDE Metodo_de_Ejecucion.dat
$ENDIF

$IFTHEN exist Tensiones_de_Barra.dat
$   INCLUDE Tensiones_de_Barra.dat
$ENDIF

$IFTHEN exist Demanda.dat
$   INCLUDE Demanda.dat
$ENDIF

$IFTHEN exist Parametros_de_Hidro.dat
$   INCLUDE Parametros_de_Hidro.dat
$ENDIF

$IFTHEN exist Parametros_de_Embalses.dat
$   INCLUDE Parametros_de_Embalses.dat
$ENDIF

$IFTHEN exist Caudales.dat
$   INCLUDE Caudales.dat
$ENDIF

$IFTHEN exist Volumenes_Meta.dat
$   INCLUDE Volumenes_Meta.dat
$ENDIF

$IFTHEN exist Tiempos_de_Viaje.dat
$   INCLUDE Tiempos_de_Viaje.dat
$ENDIF

$IFTHEN exist Generacion_Meta.dat
$   INCLUDE Generacion_Meta.dat
$ENDIF

$IFTHEN exist Restricciones_de_Embalse.dat
$   INCLUDE Restricciones_de_Embalse.dat
$ENDIF

$IFTHEN exist Funcion_de_Costo_Futuro.dat
$   INCLUDE Funcion_de_Costo_Futuro.dat
$ENDIF

$IFTHEN exist Parametros_de_Termicas.dat
$   INCLUDE Parametros_de_Termicas.dat
$ENDIF

$IFTHEN exist Condiciones_Iniciales.dat
$   INCLUDE Condiciones_Iniciales.dat
$ENDIF

$IFTHEN exist Tiempos_de_Transicion.dat
$   INCLUDE Tiempos_de_Transicion.dat
$ENDIF

$IFTHEN exist Tiempos_de_Caldero.dat
$   INCLUDE Tiempos_de_Caldero.dat
$ENDIF

$IFTHEN exist Unidades_Forzadas.dat
$   INCLUDE Unidades_Forzadas.dat
$ENDIF

$IFTHEN exist Restriccion_de_Temperatura.dat
$   INCLUDE Restriccion_de_Temperatura.dat
$ENDIF

$IFTHEN exist Disponibilidad_de_Combustible.dat
$   INCLUDE Disponibilidad_de_Combustible.dat
$ENDIF

$IFTHEN exist Parametros_de_Lineas.dat
$   INCLUDE Parametros_de_Lineas.dat
$ENDIF

$IFTHEN exist Mantenimientos.dat
$   INCLUDE Mantenimientos.dat
$ENDIF

$IFTHEN exist Restriccion_de_Suma_de_Flujos.dat
$   INCLUDE Restriccion_de_Suma_de_Flujos.dat
$ENDIF

$IFTHEN exist Coordenadas_de_Seguridad.dat
$   INCLUDE Coordenadas_de_Seguridad.dat
$ENDIF

$IFTHEN exist Generacion_No_Convencional.dat
$   INCLUDE Generacion_No_Convencional.dat
$ENDIF

$IFTHEN exist Restriccion_de_Generacion.dat
$   INCLUDE Restriccion_de_Generacion.dat
$ENDIF

$IFTHEN exist Numero_de_Arranques.dat
$   INCLUDE Numero_de_Arranques.dat
$ENDIF

$IFTHEN exist Numero_de_Paradas.dat
$   INCLUDE Numero_de_Paradas.dat
$ENDIF

$IFTHEN exist RPF_Sistema.dat
$   INCLUDE RPF_Sistema.dat
$ENDIF

$IFTHEN exist RSF_Tipo.dat
$   INCLUDE RSF_Tipo.dat
$ENDIF

$IFTHEN exist RSF_Potencia_Ofertada.dat
$   INCLUDE RSF_Potencia_Ofertada.dat
$ENDIF

$IFTHEN exist RSF_Precio_de_Ofertas.dat
$   INCLUDE RSF_Precio_de_Ofertas.dat
$ENDIF

$IFTHEN exist RSF_Total_del_Sistema.dat
$   INCLUDE RSF_Total_del_Sistema.dat
$ENDIF

$IFTHEN exist RSF_Bandas_Calificadas.dat
$   INCLUDE RSF_Bandas_Calificadas.dat
$ENDIF

$IFTHEN exist RSF_Provision_Base.dat
$   INCLUDE RSF_Provision_Base.dat
$ENDIF

$IFTHEN exist RSF_Unidades_Indisponibles.dat
$   INCLUDE RSF_Unidades_Indisponibles.dat
$ENDIF

*****************************************
*** CARGA DE SOLUCIONES INICIALES     ***
*****************************************

$IFTHEN exist Solucion_Iteracion_3.gdx
    SCALAR Solucion_3 /1/;
$ENDIF

$IFTHEN exist Solucion_Iteracion_4.gdx
    SCALAR Solucion_4 /1/;
$ENDIF

$IFTHEN exist Solucion_Iteracion_5.gdx
    SCALAR Solucion_5 /1/;
$ENDIF