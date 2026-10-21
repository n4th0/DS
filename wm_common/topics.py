"""Topics de Kafka usados por el sistema."""
FO_REQUESTS = "wm.fo.requests"      # FO      -> CENTRAL : peticiones de riego
FO_EVENTS = "wm.fo.events"          # CENTRAL -> FO      : progreso, telemetría y resumen (key = operator_id)
WS_COMMANDS = "wm.ws.commands"      # CENTRAL -> Engine  : START / STOP / BLOCK / UNBLOCK (key = ws_id)
WS_TELEMETRY = "wm.ws.telemetry"    # Engine  -> CENTRAL : STATUS / TICK / STARTED / ENDED ... (key = ws_id)
WS_STATUS = "wm.ws.status"          # CENTRAL -> todos   : estado de todas las WS (lo ven los FO)

ALL_TOPICS = [FO_REQUESTS, FO_EVENTS, WS_COMMANDS, WS_TELEMETRY, WS_STATUS]
