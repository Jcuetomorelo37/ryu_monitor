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

        # Verificar si el paquete Ethernet es válido
        if eth_pkt is None:
            return

        dst = eth_pkt.dst
        src = eth_pkt.src
    
        # Obtener el número de puerto de entrada desde el mensaje packet_in
        in_port = msg.match['in_port']
        self.logger.info("Packet in: DPID=%s, SRC=%s, DST=%s, IN_PORT=%s", dpid, src, dst, in_port)

        # Registrar el dispositivo (llama a tu función personalizada)
        self._register_device(dpid, in_port, src, pkt)
    
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
        
        