# ENGINE
#
#
#
"""
Se trata de la aplicación que implementará la lógica principal de todo el sistema.
El nombre de la aplicación será obligatoriamente “WM_WS_E”.

Para su ejecución recibirá por la línea de parámetros al menos los siguientes argumentos:
o IP y puerto del Broker/Bootstrap-server del gestor de colas.
o IP y puerto del Monitor del WS

La aplicación permanecerá a la espera hasta recibir una solicitud de servicio procedente
de la central. La WS quedará asignada con el ID recibido en la línea de parámetros del
WM_WS_M y cuya validez deberá ser confirmada por WM_Central en el proceso de
autenticación.

"""



