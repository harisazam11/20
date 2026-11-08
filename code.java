4.5 Back End
ackage org.juniper.dynamicroute;

import org.apache.felix.scr.annotations.*;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.*;
import org.onosproject.net.topology.*;
import org.onosproject.net.flow.*;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.packet.*;
import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.*;

@Component(immediate = true)
public class AppComponent {

    private final Logger log = getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected TopologyService topologyService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected FlowRuleService flowRuleService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PacketService packetService;

    private ApplicationId appId;
    private PacketProcessor processor = new ReactivePacketProcessor();

    @Activate
    protected void activate() {
        appId = coreService.registerApplication("org.juniper.dynamicroute");
        packetService.addProcessor(processor, PacketProcessor.director(2));
        requestIntercepts();
        log.info("Started Dynamic Routing App with ID {}", appId.id());
    }

    @Deactivate
    protected void deactivate() {
        withdrawIntercepts();
        packetService.removeProcessor(processor);
        log.info("Stopped");
    }

    private void requestIntercepts() {
        TrafficSelector.Builder selector = DefaultTrafficSelector.builder();
        selector.matchEthType(Ethernet.TYPE_IPV4);
        packetService.requestPackets(selector.build(), PacketPriority.REACTIVE,
appId);
    }

    private void withdrawIntercepts() {
        TrafficSelector.Builder selector = DefaultTrafficSelector.builder();
        selector.matchEthType(Ethernet.TYPE_IPV4);
        packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE,
appId);
    }

    private class ReactivePacketProcessor implements PacketProcessor {
        @Override
        public void process(PacketContext context) {
            if (context.isHandled()) return;

            InboundPacket pkt = context.inPacket();
            Ethernet ethPkt = pkt.parsed();
            if (ethPkt == null) return;

            DeviceId srcDevice = pkt.receivedFrom().deviceId();
            ConnectPoint src = pkt.receivedFrom();
            MacAddress dstMac = ethPkt.getDestinationMAC();
            Set<Path> paths =
topologyService.getPaths(topologyService.currentTopology(), srcDevice,
findDstDevice(dstMac));
            if (paths.isEmpty()) return;

            Path bestPath = selectLeastCongestedPath(paths);
            if (bestPath != null) {
                installRule(bestPath.src().deviceId(), dstMac, bestPath.src().port());
                installRule(bestPath.dst().deviceId(), dstMac, bestPath.dst().port());
                context.treatmentBuilder().setOutput(bestPath.src().port());
                context.send();
            }
        }
    }

    private DeviceId findDstDevice(MacAddress dstMac) {
        return DeviceId.deviceId("of:0000000000000006");
    }

    private Path selectLeastCongestedPath(Set<Path> paths) {
        return paths.stream()
            .min(Comparator.comparingInt(Path::cost))
            .orElse(null);
    }

    private void installRule(DeviceId deviceId, MacAddress dstMac, PortNumber
port) {
        TrafficSelector selector = DefaultTrafficSelector.builder()
            .matchEthType(Ethernet.TYPE_IPV4)
            .matchEthDst(dstMac)
            .build();

        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
            .setOutput(port)
            .build();

        FlowRule rule = DefaultFlowRule.builder()
            .forDevice(deviceId)
            .withSelector(selector)
            .withTreatment(treatment)
            .withPriority(500)
            .fromApp(appId)
            .makeTemporary(30)
            .build();

        flowRuleService.applyFlowRules(rule);
    }
}