import { IDerivation, Document, SourceFromImpressions, SourceFromClicks } from 'flow/Dani/demo-ads/ad-clicks-by-platform.ts';

// Implementation for derivation Dani/demo-ads/ad-clicks-by-platform.
export class Derivation extends IDerivation {
    fromImpressions(read: { doc: SourceFromImpressions }): Document[] {
        return [{
            platform: read.doc.platform || "",
            click_count: 0  // Initialize click count to 0 for impressions
        }];
    }

    fromClicks(read: { doc: SourceFromClicks }): Document[] {
        return [{
            platform: read.doc.platform || "",
            click_count: 1  // Increment click count by 1 for each click
        }];
    }
}
