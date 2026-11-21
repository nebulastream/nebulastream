package nes.calcite.physical;

import java.util.List;
import org.apache.calcite.rel.RelNode;

/** A converter rule that explains why it produced its physical operators (the step "physical operators", Conversions). */
public interface ExplainedRule {
    /** produced: the physical operators the rule created for the logical operator (not those of its inputs). */
    String reason(RelNode logical, List<RelNode> produced);
}
