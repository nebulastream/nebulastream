/**
 * Annotates brat entities in a text by wrapping them in a span with the class of its entity.
 *
 * Example: input: "A4 Aachen Richtung Köln."
 *         output: "A4
 *                  <span data-entity="wikipedia/de/Aachen" class="entity-wikipedia">Aachen</span>
 *                  Richtung
 *                  <span data-entity="wikipedia/de/Köln" class="entity-wikipedia">Köln</span>." 
 *
 * Usage: new Textannotator().annotate(bratAnnotation)
 *
 * Note: bratAnnotation is a brat annotation object, that has been parsed from JSON and
 *       the implementation relies on this specific structure.
 *       I.e. locations of entities in the string might be encoded differently.
 */
const TextAnnotator = function() {

    this.annotate = function(bratAnnotation) {
        let entityLinkingResult = bratAnnotation.result.entityLinking;
        let relationExtractionResults = bratAnnotation.result.relationExtractionDFKI;
        let sourceText = entityLinkingResult.text;

        let entitySpans   = this.computeEntitySpans(entityLinkingResult);
        let relationSpans = this.computeRelationSpans(relationExtractionResults);

        let spans = entitySpans.concat(relationSpans);
        let orderedSpans = this.orderSpans(spans);

        let nestedSpanTags = orderedSpans.map(function (spanNode) {
            return spanNode.convertIntoTags()
        });
        let flattenedSpanTags = nestedSpanTags.reduce(function(arr, spanTag) {
            return arr.concat(spanTag[0]).concat(spanTag[1]);
        }, []);
        let orderedSpanTags = this.orderSpanTags(flattenedSpanTags);
        let splitSpanTags = new SpanTagSplitter().processSpanTags(orderedSpanTags);

        return this.annotateTextWithSpans(sourceText, splitSpanTags);
    };

    this.computeEntitySpans = function(entityLinkingResult) {
        if (entityLinkingResult == null)
            return [];

        return entityLinkingResult.entities.map(function(entity) {
            let entityType = entity[1];
            return new EntitySpanNode(entityType, entity[2][0]);
        }.bind(this));
    };

    this.computeRelationSpans = function(relationExtractionResults) {
        if (relationExtractionResults == null)
            return [];

        return relationExtractionResults.entities.map(function(relation) {
            let patternId = relation[0];
            let relationType = relation[1];
            let startStopIndexes = relation[2][0];
            return new RelationSpanNode(patternId, relationType, startStopIndexes);
        }.bind(this));
    };

    /**
     * Order spans by starting position and assert, that bigger spans are before smaller ones.
     * Thereby bigger spans will be wrapping smaller ones.
     */
    this.orderSpans = function(spans) {
        return spans.sort(function(s1, s2) {
            if (s1.startsAt != s2.startsAt)
                return s1.startsAt - s2.startsAt;
            else
                return s2.endsAt - s1.endsAt;
        });
    };

    /**
     * Order span tags by position and assert, that closing tags are before opening tags.
     * Also, assert that span tags for entities are before span tags for relations if they are of the same size.
     */
    this.orderSpanTags = function(spanTags) {
        return spanTags.sort(function(st1, st2) {
            if (st1.position != st2.position)
                return st1.position - st2.position;
            else if (st1.isOpeningTag != st2.isOpeningTag) {
                return -(st1.isOpeningTag - st2.isOpeningTag);
            } else  {
                return (st1.isRelation - st2.isRelation);
            }
        });
    };

    this.annotateTextWithSpans = function(text, spanTags) {
        let annotatedText = "";
        let lastTextIndex = 0;
        spanTags.forEach(function (spanTag) {
            let currentTextIndex = spanTag.position;
            annotatedText += text.substring(lastTextIndex, currentTextIndex);
            annotatedText += spanTag.toHtmlString();
            lastTextIndex = currentTextIndex;
        });
        annotatedText += text.substring(lastTextIndex, text.length);
        return annotatedText;
    };
};

const EntitySpanNode = function(entityType, location) {
    this.spanClass  = MapPopupBuilder.getEntityClassName(entityType);
    this.startsAt   = location[0];
    this.endsAt     = location[1];
    this.isRelation = false;

    this.convertIntoTags = function() {
        let stringSpanClass = this.spanClass;
        return [
            new SpanTag(true, stringSpanClass, this.startsAt, ""),
            new SpanTag(false, stringSpanClass, this.endsAt , "")
        ]
    };
};

const RelationSpanNode = function(patternId, relationType, location) {
    this.spanClass     = "relation-annotation";
    this.relationTypes = [relationType];
    this.patternIds    = [patternId];
    this.startsAt      = location[0];
    this.endsAt        = location[1];
    this.isRelation    = true;

    this.convertIntoTags = function() {
        let relationTypesString = this.relationTypes.join(",");
        let patternIdsString = this.patternIds.join(",");
        return [
            new SpanTag(true, this.spanClass, this.startsAt, ""),
            new SpanTag(false, this.spanClass, this.endsAt , "(" + relationTypesString + ":" + patternIdsString + ")")
        ]
    };
};

const SpanTag = function(isOpeningTag, spanClass, position, tagString) {
    this.isOpeningTag = isOpeningTag;
    this.spanClass = spanClass;
    this.position = position;
    this.tagString = tagString;
    this.isRelation = spanClass === "relation-annotation";

    this.toHtmlString = function() {
        if (this.isOpeningTag) {
            return "<span class='" + this.spanClass + "'>";
        } else {
            return this.tagString + "</span>";
        }
    }
};

/**
 * Breaks up overlapping spans into multiple spans, by creating spans for the overlapping regions, that have the
 * union of the class attributes.
 *
 * Example:
 * Input:  <span class="A"> aaa <span class="B"> bbb </span class="A"> ccc </span class="B">
 * Output: <span class="A"> aaa </span><span class="A B"> bbb </span><span class="B"> ccc </span>
 */
const SpanTagSplitter = function() {
    this.openSpans = {};
    this.splitSpanTags = [];

    this.processSpanTags = function (spanTags) {
        spanTags.forEach(function (span) {
            if (span.isOpeningTag) {
                // if there are open spans:
                //   push to splitSpanTags: closing span for openSpans
                if (this.hasOpenSpans())
                    this.pushClosingSpan(span, this.openSpanClasses(), "");

                // push span class to openSpans
                this.addSpanClass(span.spanClass);

                // push to splitSpanTags: opening span with union of classes
                this.pushOpeningSpan(span, this.openSpanClasses());
            } else {
                // remove span class from openSpans
                this.removeSpanClass(span.spanClass);

                // create closing span
                this.pushClosingSpan(span, span.spanClass, span.tagString);

                // if there are open spans:
                //   push to splitSpanTags: opening span with union of classes
                if (this.hasOpenSpans())
                    this.pushOpeningSpan(span, this.openSpanClasses());
            }
        }.bind(this));

        if (this.hasOpenSpans())
            console.warn("Validation error: span imbalance");

        return this.splitSpanTags;
    };

    this.openSpanClasses = function () {
        return Object.keys(this.openSpans).join(" ");
    };

    this.hasOpenSpans = function () {
        if (this.openSpans === {}) {
            return false;
        } else {
            let numOpenSpans = Object.keys(this.openSpans)
                .map(function(key) { return this.openSpans[key] }.bind(this))
                .reduce(function(val1, val2) { return val1 + val2 }, 0);
            return numOpenSpans != 0;
        }
    };
    this.addSpanClass = function (spanClass) {
        if (this.openSpans[spanClass] == null)
            this.openSpans[spanClass] = 1;
        else
            this.openSpans[spanClass] = this.openSpans[spanClass] + 1;
    };

    this.removeSpanClass = function (spanClass) {
        if (this.openSpans[spanClass] == null)
            console.warn("Validation error: span imbalance");
        else
            this.openSpans[spanClass] = this.openSpans[spanClass] - 1;

        if (this.openSpans[spanClass] < 0)
            console.warn("Validation error: span imbalance");
        else if (this.openSpans[spanClass] === 0)
            delete this.openSpans[spanClass];
    };

    this.pushClosingSpan = function (currentSpan, spanClasses, tagString) {
        this.splitSpanTags.push(new SpanTag(false, spanClasses, currentSpan.position, tagString));
    };

    this.pushOpeningSpan = function (currentSpan, spanClasses) {
        this.splitSpanTags.push(new SpanTag(true, spanClasses, currentSpan.position, ""));
    };
};
