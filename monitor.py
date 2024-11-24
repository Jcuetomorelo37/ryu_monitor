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
import itertools

class TrafficMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(TrafficMonitor, self).__init__(*args, **kwargs)
        self.port_iterator = {}
        
        logging.basicConfig(level=logging.INFO)
        self.datapaths = {}
        self.metrics = {
            "cpu": 0,
            "memory": 0,
            "switches": {},
            "devices": [{"ip": "10.0.0.1", "mac":"2R:D5:42:56:FR:4C", "switch":"S1", "port":"1", "band": "23", "priority":"42"},
                        {"ip": "10.0.0.2", "mac":"2R:D5:42:56:FR:4D", "switch":"S1", "port":"2", "band": "11", "priority":"11"},
                        {"ip": "10.0.0.3", "mac":"2R:D5:42:56:FR:4E", "switch":"S1", "port":"3", "band": "19", "priority":"4"},
                        {"ip": "10.0.0.4", "mac":"2R:D5:42:56:FR:4F", "switch":"S1", "port":"4", "band": "7", "priority":"8"},
                        {"ip": "10.0.0.5", "mac":"2R:D5:42:56:FR:4G", "switch":"S1", "port":"5", "band": "5", "priority":"2"}
                        ],
            "events": [],
            "notifications": []
        }
        self.mac_to_port = {}
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
        datapath = ev.datapath
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
                self.logger.info("Switch %s conectado", datapath.id)
                self.setup_qos(datapath, port=1, max_rate=500, min_rate=100)
                self.metrics["notifications"].append(
                    {"timestamp": timestamp, "title": "Aplicacion de QoS", "subtitle": f"aplicada QoS en el switch{datapath.id}", "description": f"Se ha aplicado correctamente la politica de QoS a {datapath}"}
                )
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
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        dst = eth.dst
        src = eth.src
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        self.mac_to_port[dpid][src] = in_port

        self._register_device(dpid, in_port, src, pkt)

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = self.balancear_trafico(datapath, dst)

        # Agregar lógica para asignar colas QoS
        queue_id = 1  # Cambia el ID de la cola según tus necesidades
        actions = [
            parser.OFPActionSetQueue(queue_id=queue_id),  # Asignar el flujo a la cola QoS
            parser.OFPActionOutput(out_port)             # Salida al puerto correspondiente
        ]
    
        self.logger.info(f"Match para flujo: in_port={in_port}, eth_src={src}, eth_dst={dst}")
        self.logger.info(f"out_port={out_port}, in_port={in_port}, src={src}, dst={dst}")
        self.logger.info(f"Instalando flujo: in_port={in_port}, dst={dst}, src={src}, out_port={out_port}")
        
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, 1, match, actions)
    
        out = parser.OFPPacketOut(
            datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=msg.data
            )
        datapath.send_msg(out)
        
    def _register_device(self, dpid, port, mac, pkt):
        dispositivo_registrado = False
        
        for p in pkt.protocols:
            if isinstance(p, arp.arp):
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
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
    
        # Crear las instrucciones de flujo (aplicar acciones)
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        
        # Crear la modificación de flujo (OFPFlowMod)
        mod = parser.OFPFlowMod(
           datapath=datapath, priority=priority, match=match, instructions=inst
        )
    
        # Log para depuración
        self.logger.info(f"Instalando flujo: {mod}")
        print(f"Instalando flujo... {mod}")
    
        # Enviar el mensaje para instalar el flujo
        datapath.send_msg(mod)
    
    def send_notification(self, port, max_rate, min_rate, burst_size, notifications):
        self.metrics["devices"][notifications] = {
                    "message": f"QoS configurado en puerto:{port}, max_rate={max_rate}, min_rate={min_rate}, burst_size={burst_size}",
                }
    #============================================================================================
    def balancear_trafico(self, datapath, dst):
        dpid = datapath.id
        ofproto = datapath.ofproto

        if dpid not in self.metrics["switches"]:
            if dpid not in self.port_iterator:
                self.port_iterator[dpid] = itertools.cycle(
                    port for port in range(1, 50) if port not in [ofproto.OFPP_FLOOD, ofproto.OFPP_CONTROLLER]
                )
            return next(self.port_iterator[dpid])

        puertos_estadisticas = self.metrics["switches"].get(dpid, {})
        puerto_seleccionado = min(
            puertos_estadisticas.items(),
            key=lambda item: item[1]["tx_bytes"],
            default=(ofproto.OFPP_FLOOD, {})
        )[0]

        return puerto_seleccionado if puerto_seleccionado != ofproto.OFPP_FLOOD else ofproto.OFPP_CONTROLLER
    
    def setup_qos(self, datapath, port, max_rate=None, min_rate=None, burst_size=None):
         ofproto = datapath.ofproto
         parser = datapath.ofproto_parser
    
        # Si no se especifica tasa máxima, usar un valor por defecto
         if max_rate is None:
             max_rate = 1000  # 100% de la capacidad

        # Definir una cola de QoS con propiedades específicas
         queue_id = 1  # ID de la cola (puedes usar diferentes IDs de colas)
         actions = []
     
        # Crear una lista de propiedades de la cola si se especifican tasas
         if max_rate is not None:
             actions.append(parser.OFPActionSetQueue(queue_id=queue_id))  # Asignar la cola de QoS
    
        # Aquí podemos agregar más reglas si se requiere aplicar alguna política de QoS adicional
        # Los valores de max_rate y min_rate se manejarían en las reglas de flujo
    
        # Crear un flujo para asignar QoS en el puerto especificado
         match = parser.OFPMatch(in_port=port)
         inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
    
        # Crear el mensaje de flujo
         mod = parser.OFPFlowMod(datapath=datapath, match=match, priority=1, instructions=inst)
    
        # Enviar el mensaje para instalar el flujo
         datapath.send_msg(mod)
    
         self.logger.info(
             "QoS configurado en puerto %s: max_rate=%s, min_rate=%s, burst_size=%s",
             port, max_rate, min_rate, burst_size,
         )
         
     
    #============================================================================================
    
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, ev):
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
