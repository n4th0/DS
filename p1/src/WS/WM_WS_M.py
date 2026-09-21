# MONITOR
#



"""
Aplicación que simula un módulo de gestión de observación de todo el WS (hardware y
software) velando por la seguridad y correcto funcionamiento de este sin incidencias. El nombre
de la aplicación será obligatoriamente “WM_WS_M”. Para su ejecución recibirá por la línea de
parámetros al menos los siguientes argumentos:

o Puerto del servidor de sockets que ofrece a WM_WS_E
o IP y puerto del WM_Central
o ID de la WS: Identificador único con que esa WS está dada de alta en la red.

Al arrancar la aplicación, esta se conectará al WM_Central para autenticarse y validar
que el WS está operativo y preparado para prestar servicios. De la misma forma, esperará la
conexión del Engine del WS al que pertenece y empezará a enviarle cada segundo un mensaje
de comprobación de estado de salud. Si no se recibe respuesta desde el WM_WS_E o se recibe
una respuesta tipo KO, WM_WS_M enviará a WM_Central un mensaje de avería simulando una
detección de fuga como se ha indicado en anteriores apartados. Para simular dichas incidencias,
la aplicación WM_WS_E deberá permitir que, en tiempo de ejecución, se pulse una tecla para
reportar un KO al monitor.


"""
