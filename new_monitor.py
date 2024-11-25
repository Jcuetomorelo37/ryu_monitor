from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet, arp, ipv4
from ryu.lib import hub
import itertools
import psutil
import logging
import time
import threading
from flask import Flask, jsonify

# import time
# from ryu.lib.packet import arp, ipv4

class ExampleSwitch13(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ExampleSwitch13, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.datapaths = {}
        self.port_iterator = {}
        self.metrics = {
            "cpu": 0,
            "memory": 0,
            "switches": {},
            "devices": [
                        #   {"ip": "192.168.1.1", "mac": "00:11:22:33:44:55", "switch": "1", "port": "1", "band": 0.0, "priority": "N/A"},
                        #   {"ip": "192.168.1.2", "mac": "00:11:22:33:44:56", "switch": "1", "port": "2", "band": 0.0, "priority": "N/A"},
                        #   {"ip": "192.168.1.3", "mac": "00:11:22:33:44:57", "switch": "2", "port": "1", "band": 0.0, "priority": "N/A"},
                        #   {"ip": "192.168.1.4", "mac": "00:11:22:33:44:58", "switch": "3", "port": "1", "band": 0.0, "priority": "N/A"},
                        #   {"ip": "192.168.1.5", "mac": "00:11:22:33:44:59", "switch": "3", "port": "2", "band": 0.0, "priority": "N/A"}
                        ],
            "events": [],
            "notifications": []
        }
        self.monitor_thread = hub.spawn(self._monitor)
        threading.Thread(target=self.start_http_server, daemon=True).start()

    def start_http_server(self):
        app = Flask(__name__)

        @app.route('/metrics', methods=['GET'])
        def get_metrics():
            return jsonify(self.metrics)

        app.run(host='0.0.0.0', port=5000)

    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            self._log_system_stats()
            self._calculate_bandwidth()
            hub.sleep(3)

    def _log_system_stats(self):
        cpu_usage = psutil.cpu_percent()
        memory_info = psutil.virtual_memory()
        self.metrics["cpu"] = cpu_usage
        self.metrics["memory"] = memory_info.percent
        self.logger.info('CPU Usage: %s%%', cpu_usage)
        self.logger.info('Memory Usage: %s%% (%s MB used)', memory_info.percent, memory_info.used / (1024 ** 2))

    def _request_stats(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

    def _calculate_bandwidth(self):
        for switch_id, ports in self.metrics["switches"].items():
            for port_no, stats in ports.items():
                tx_bytes = stats.get("tx_bytes", 0)
                rx_bytes = stats.get("rx_bytes", 0)

                bandwidth = (tx_bytes + rx_bytes) * 8
                bandwidth_mbps = round((bandwidth / 1024 / 1024), 2)

                for device in self.metrics["devices"]:
                    if device["switch"] == f"{str(switch_id)}" and device["port"] == str(port_no):
                        device["band"] = bandwidth_mbps
                        self.logger.info(
                            "Ancho de banda actualizado: switch=%s, puerto=%s, banda=%s Mbps",
                            switch_id, port_no, bandwidth_mbps
                        )

    @set_ev_cls(ofp_event.EventOFPStateChange, [CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        if ev.state == MAIN_DISPATCHER:
            self.datapaths[datapath.id] = datapath
            self.logger.info("Switch %s conectado", datapath.id)
        elif ev.state == 'DEAD_DISPATCHER':
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]
                self.logger.info("Switch %s desconectado", datapath.id)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
    
        # Obtener el ID del Datapath para identificar los switches OpenFlow
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})
    
        # Analizar los paquetes recibidos usando la librería packet
        pkt = packet.Packet(msg.data)
        eth_pkt = pkt.get_protocol(ethernet.ethernet)

        #Verificar si el paquete Ethernet es válido
        if eth_pkt is None:
            return

        dst = eth_pkt.dst
        src = eth_pkt.src
    
        # Obtener el número de puerto de entrada desde el mensaje packet_in
        in_port = msg.match['in_port']
        self.logger.info("Packet in: DPID=%s, SRC=%s, DST=%s, IN_PORT=%s", dpid, src, dst, in_port)

        # Registrar el dispositivo (llama a tu función personalizada)
        #self._register_device(dpid, in_port, src, pkt)
        if not eth_pkt.dst.startswith("ff:ff:ff:ff:ff:ff"):
            self._register_device(dpid, in_port, eth_pkt.src, pkt)
        
        # Aprender dirección MAC para evitar flooding en el futuro
        self.mac_to_port[dpid][src] = in_port

        # Decidir a qué puerto enviar el paquete, o hacer flood si no se conoce el destino
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        # Construir la lista de acciones
        actions = [parser.OFPActionOutput(out_port)]
    
        # Instalar un flujo para evitar packet_in la próxima vez
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, 1, match, actions)
    
        # Construir el mensaje packet_out y enviarlo
        out = parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=actions,
            data=msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
        )
        datapath.send_msg(out)

    def _register_device(self, dpid, port, mac, pkt):
        self.metrics["devices"] = [device for device in self.metrics["devices"] if not (device["mac"] == mac and device["switch"] == str(dpid) and device["port"] == str(port))]

        dispositivo_registrado = False

        # Recorre los protocolos del paquete para encontrar ARP o IPv4.
        for p in pkt.protocols:
            if isinstance(p, arp.arp):  # Protocolo ARP
                #ip = p.src_ip
                ip = p.src_ip if hasattr(p, 'src_ip') else None
                self.metrics["devices"].append({
                    "ip": ip,
                    "mac": mac,
                    "switch": str(dpid),
                    "port": str(port),
                    "band": 0.0,
                    "priority": "N/A"
                })
                self.logger.info(
                    "Dispositivo detectado (ARP): MAC=%s, IP=%s, Switch=%s, Puerto=%s",
                    mac, ip, dpid, port
                )
                dispositivo_registrado = True

            elif isinstance(p, ipv4.ipv4):  # Protocolo IPv4
                ip = p.src
                self.metrics["devices"].append({
                    "ip": ip,
                    "mac": mac,
                    "switch": str(dpid),
                    "port": str(port),
                    "band": 0.0,
                    "priority": "N/A"
                })
                self.logger.info(
                    "Dispositivo detectado (IPv4): MAC=%s, IP=%s, Switch=%s, Puerto=%s",
                    mac, ip, dpid, port
                )
                dispositivo_registrado = True
            
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                self.metrics["notifications"].append({
                   "timestamp": timestamp,
                   "title": "Dispositivo registrado",
                   "subtitle": f"MAC={mac}",
                   "description": f"Registrado en Switch {dpid}, Puerto {port}"
                })

        if not dispositivo_registrado:
            self.logger.warning(
                "No se detectaron dispositivos en el switch=%s, puerto=%s, paquete=%s",
                dpid, port, pkt
            )
            self.logger.debug(
                "Paquete sin ARP ni IPv4 detectado. Protocolos presentes: %s",
                [type(protocol).__name__ for protocol in pkt.protocols]
            )


    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        self.add_table_miss_flow(datapath)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        
        self._apply_qos(datapath=datapath, match_fields={}, priority=1, queue_id=0)

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, ev):
        body = ev.msg.body
        switch_id = ev.msg.datapath.id
        self.metrics["switches"].setdefault(switch_id, {})

        for stat in sorted(body, key=lambda x: x.port_no):
            self.metrics["switches"][switch_id][stat.port_no] = {
                "rx_packets": stat.rx_packets,
                "tx_packets": stat.tx_packets,
                "rx_bytes": stat.rx_bytes,
                "tx_bytes": stat.tx_bytes,
            }
      
    def _apply_qos(self, datapath, match_fields, priority, queue_id):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # Crear un objeto OFPMatch usando los campos proporcionados.
        match = parser.OFPMatch(**match_fields)

        # Configurar la acción para enviar tráfico a una cola específica (QoS).
        actions = [parser.OFPActionSetQueue(queue_id), parser.OFPActionOutput(ofproto.OFPP_NORMAL)]

        # Crear la instrucción y el flujo.
        instructions = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        flow_mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=priority,
            match=match,
            instructions=instructions
        )
        
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        switch_id = datapath.id
        notification = {
          "timestamp": timestamp,
           "title": "Política de QoS aplicada",
           "subtitle": f"Switch {switch_id}, Cola {queue_id}",
           "description": f"QoS aplicado con prioridad {priority} al tráfico que coincide con {match_fields or 'cualquier tráfico'}."
          }
        self.metrics["notifications"].append(notification)

        # # Log del evento
        # self.logger.info("QoS aplicado en Switch %s: %s", switch_id, notification)
    
        # # Enviar el mensaje al switch.
        # datapath.send_msg(flow_mod)
        # self.logger.info(
        #     "Regla QoS aplicada: %s con prioridad %d y cola %d",
        #     match_fields, priority, queue_id
        # )
        
    def add_table_miss_flow(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()  # Sin coincidencia, captura todo.
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=0,
            match=match,
            instructions=inst
        )
        datapath.send_msg(mod)

