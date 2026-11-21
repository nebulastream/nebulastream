package nes.calcite.logical;

import org.apache.calcite.rel.RelNode;

/**
 * An operator whose rows may carry watermarks of their own (a window operator, a watermark assigner, a network source):
 * a window above it needs no assigner (PhysicalRules.watermarked).
 */
public interface CarriesWatermarks extends RelNode {
    boolean watermarked();
}
