import logging
import psutil
import time
import threading
from flask import Flask, jsonify
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, CONFIG_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet, arp, ipv4
from ryu.lib.packet import ether_types

class TrafficMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(TrafficMonitor, self).__init__(*args, **kwargs)
        
        # Configuración del logger
        logging.basicConfig(level=logging.INFO)
        self.datapaths = {}
        self.metrics = {
            "cpu": 0,
            "memory": 0,
            "switches": {},
            "devices": {},  # Registro de dispositivos finales
            "events": []
        }
        self.mac_to_port = {}  # Diccionario para aprender direcciones MAC
        self.monitor_thread = hub.spawn(self._monitor)
        threading.Thread(target=self.start_http_server, daemon=True).start()

    def start_http_server(self):
        app = Flask(__name__)

        @app.route('/metrics', methods=['GET'])
        def get_metrics():
            return jsonify(self.metrics)

        app.run(host='0.0.0.0', port=5000)

    def _monitor(self):
        """Monitoriza periódicamente estadísticas de switches y recursos del sistema."""
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            self._log_system_stats()
            hub.sleep(10)

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

    @set_ev_cls(ofp_event.EventOFPStateChange, [CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def _state_change_handler(self, ev):
        """Registra switches solo si se encuentran en un estado válido."""
        datapath = ev.datapath
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
                self.logger.info("Switch %s conectado", datapath.id)
                self.metrics["events"].append(
                    {"timestamp": timestamp, "event": f"Switch {datapath.id} conectado"}
                )
        elif ev.state == 'DEAD_DISPATCHER':
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]
                self.logger.info("Switch %s desconectado", datapath.id)
                self.metrics["events"].append(
                    {"timestamp": timestamp, "event": f"Switch {datapath.id} desconectado"}
                )

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        """Gestiona paquetes, reenvía tráfico y registra dispositivos conectados."""
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)

        #Ignorar paquetes no Ethernet
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        dst = eth.dst
        src = eth.src
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        # Aprende la dirección MAC de la fuente
        self.mac_to_port[dpid][src] = in_port

        # Registrar información del dispositivo conectado
        self._register_device(dpid, in_port, src, pkt)

        # Verifica si conoce la salida para la dirección MAC de destino
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        # Crea acciones de reenvío
        actions = [parser.OFPActionOutput(out_port)]

        # Verifica los valores antes de instalar el flujo
        self.logger.info(f"Match para flujo: in_port={in_port}, eth_src={src}, eth_dst={dst}")
        self.logger.info(f"out_port={out_port}, in_port={in_port}, src={src}, dst={dst}")
        self.logger.info(f"Instalando flujo: in_port={in_port}, dst={dst}, src={src}, out_port={out_port}")
        
        # Instala una regla de flujo si conoce el puerto de salida
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, 1, match, actions)
        # match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
        # self.add_flow(datapath, 1, match, actions)

        # Envía el paquete al puerto de salida
        out = parser.OFPPacketOut(
            datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=msg.data)
        datapath.send_msg(out)

    def _register_device(self, dpid, port, mac, pkt):
        """Registra dispositivos conectados a un switch."""
        dispositivo_registrado = False
        
        for p in pkt.protocols:
            if isinstance(p, arp.arp):
                # Si es un paquete ARP, registrar MAC e IP
                ip = p.src_ip
                self.metrics["devices"][mac] = {
                    "ip": ip,
                    "switch": dpid,
                    "port": port
                }
                self.logger.info(
                    "Dispositivo detectado: MAC=%s, IP=%s, Switch=%s, Puerto=%s",
                    mac, ip, dpid, port
                )
                dispositivo_registrado = True
            elif isinstance(p, ipv4.ipv4):
                # Si es un paquete IPv4, registrar MAC e IP
                ip = p.src
                self.metrics["devices"][mac] = {
                    "ip": ip,
                    "switch": dpid,
                    "port": port
                }
                self.logger.info(
                    "Dispositivo detectado: MAC=%s, IP=%s, Switch=%s, Puerto=%s",
                    mac, ip, dpid, port
                )
                dispositivo_registrado = True
                
            if not dispositivo_registrado:
                self.logger.warning(
                    "No se detectaron dispositivos en el switch=%s, puerto=%s, paquete=%s",
                    dpid, port, pkt
                )

    def add_flow(self, datapath, priority, match, actions):
        """Agrega una entrada de flujo a la tabla del switch."""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(
           datapath=datapath, priority=priority, match=match, instructions=inst
        )

        self.logger.info(f"Instalando flujo: {mod}")
        print(f"Instalando flujo... {mod}")
        
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, ev):
        """Procesa estadísticas de puertos únicamente para switches conectados."""
        body = ev.msg.body
        switch_id = ev.msg.datapath.id
        self.metrics["switches"].setdefault(switch_id, {})

        for stat in sorted(body, key=lambda x: x.port_no):
            self.logger.info(
                'Switch ID=%s Port=%d: RX packets=%d TX packets=%d RX bytes=%d TX bytes=%d',
                switch_id, stat.port_no, stat.rx_packets, stat.tx_packets, stat.rx_bytes, stat.tx_bytes
            )
            self.metrics["switches"][switch_id][stat.port_no] = {
                "rx_packets": stat.rx_packets,
                "tx_packets": stat.tx_packets,
                "rx_bytes": stat.rx_bytes,
                "tx_bytes": stat.tx_bytes,
            }
