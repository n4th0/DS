# OPERATOR
"""
Aplicación que disponen los usuarios del sistema para solicitar un suministro. El nombre
de la aplicación será obligatoriamente “WM_FO”. Para su ejecución recibirá por la línea de
parámetros al menos los siguientes argumentos:
o IP y puerto del Broker/Bootstrap-server del gestor de colas.
o ID del operador: Identificador único con que ese operador está dado de alta en
CENTRAL.
Esta aplicación, además de poder solicitar un suministro puntualmente en un WS, podrá
leer un fichero todos los servicios que va a solicitar. Enviará el primero a WM_Central y, tras su
conclusión (sea en éxito o fracaso), esperará 4 segundos y pasará a solicitar el siguiente servicio.
"""
