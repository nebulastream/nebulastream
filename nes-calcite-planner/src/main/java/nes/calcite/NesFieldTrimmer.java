package nes.calcite;

import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

/**
 * Calcite's RelFieldTrimmer, which also trims our sources: it trims a table by a projection above its TableScan, but
 * after the source expansion (NesLogical.expandSources) and for anonymous sources, the plan reads our own scans, which it
 * otherwise keeps whole. Every column then travels through the plan, e.g. into the state of each join. Here a source gets
 * a projection of the columns the query uses; later steps push it into the source's pipeline.
 *
 * <p>RelFieldTrimmer calls the trimFields method for the node's class by reflection (these are public for that).
 */
public final class NesFieldTrimmer extends RelFieldTrimmer {
    private final RelBuilder relBuilder;

    NesFieldTrimmer(SqlValidator validator, RelBuilder relBuilder) {
        super(validator, relBuilder);
        this.relBuilder = relBuilder;
    }

    public TrimResult trimFields(NesLogical.PhysicalSourceScan scan, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return trimSource(scan, fieldsUsed);
    }

    public TrimResult trimFields(AnonymousSources.NesAnonymousSource source, ImmutableBitSet fieldsUsed,
                                 Set<RelDataTypeField> extraFields) {
        return trimSource(source, fieldsUsed);
    }

    /** The source under a projection of the used fields (at least one, as a row needs a field). */
    private TrimResult trimSource(RelNode source, ImmutableBitSet fieldsUsed) {
        int fieldCount = source.getRowType().getFieldCount();
        ImmutableBitSet used = fieldsUsed.isEmpty() ? ImmutableBitSet.of(0) : fieldsUsed;
        if (used.cardinality() == fieldCount) {
            return result(source, Mappings.createIdentity(fieldCount));
        }
        Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, used.cardinality());
        int target = 0;
        for (int field : used) {
            mapping.set(field, target++);
        }
        relBuilder.push(source);
        relBuilder.project(relBuilder.fields(used.asList()));
        return result(relBuilder.build(), mapping);
    }
}
