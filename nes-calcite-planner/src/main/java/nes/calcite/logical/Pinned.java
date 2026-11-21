package nes.calcite.logical;

import org.apache.calcite.rel.RelNode;

/**
 * An operator that runs on a given worker and costs none of its capacity (Placement): a source (NesSource), or the
 * watermark assigner right after one.
 */
public interface Pinned extends RelNode {
    /** The worker that runs it, or null if it is not pinned (e.g. an assigner that is not directly after a source). */
    String host();

    /** What it is, for errors (e.g. SOURCE(name)). */
    String label();
}
