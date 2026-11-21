package nes.calcite.logical;

import org.apache.calcite.rel.RelNode;

/**
 * An operator that passes on each row of its input, with the input's watermarks (as Calcite's Filter, Project and Union),
 * e.g. NesModelInference.
 */
public interface PassesRows extends RelNode {
}
