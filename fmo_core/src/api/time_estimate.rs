//! Shared resolution of an explorer item's estimated wall-clock time into a
//! point estimate plus an uncertainty interval and a provenance tag.
//!
//! Two independent time sources feed this:
//!  - `synced_at`: the exact time an item was first observed live (only set on
//!    the live path; see `ingest::ingest_items`). When present the time is
//!    exactly known.
//!  - the session's `(estimated_session_timestamp, next_vote_time)` bracket
//!    (see `db::session_times`): the nearest votes at-or-before and at-or-after
//!    the session, forward- and backward-filled. For a directly-voted session
//!    both equal the vote (a zero-width interval); for a vote-less session they
//!    bracket its true time; the upper bound may be absent for sessions more
//!    recent than the last known vote.

use chrono::NaiveDateTime;

/// Point estimate + interval + provenance for one item's time, all in epoch
/// seconds. Mirrors the `estimated_time` / `time_lower` / `time_upper` /
/// `time_source` fields on `fmo_api_types::SessionItem`.
pub(crate) struct ResolvedTime {
    pub estimated_time: Option<i64>,
    pub time_lower: Option<i64>,
    pub time_upper: Option<i64>,
    pub time_source: Option<&'static str>,
}

fn epoch(ts: NaiveDateTime) -> i64 {
    ts.and_utc().timestamp()
}

/// Resolves the three time inputs into a display-ready estimate.
///
/// `synced_at` is the item's own first-seen stamp (from its `transactions` /
/// `consensus_items` row); `estimated` / `next_vote` are its session's
/// forward- / backward-filled vote bounds. `estimated` (lower) and `next_vote`
/// (upper) are only consulted when `synced_at` is absent.
pub(crate) fn resolve_time(
    synced_at: Option<NaiveDateTime>,
    estimated: Option<NaiveDateTime>,
    next_vote: Option<NaiveDateTime>,
) -> ResolvedTime {
    if let Some(observed) = synced_at {
        // Exact, live-observed time: a zero-width interval.
        let epoch = epoch(observed);
        return ResolvedTime {
            estimated_time: Some(epoch),
            time_lower: Some(epoch),
            time_upper: Some(epoch),
            time_source: Some("observed"),
        };
    }

    let Some(lower_ts) = estimated else {
        // No time information at all.
        return ResolvedTime {
            estimated_time: None,
            time_lower: None,
            time_upper: None,
            time_source: None,
        };
    };

    let lower = epoch(lower_ts);
    match next_vote {
        Some(upper_ts) => {
            let upper = epoch(upper_ts);
            // A directly-voted session has lower == upper (the vote itself);
            // a vote-less session forward-filled between two votes has a real
            // spread. Midpoint is the point estimate for the latter.
            let (estimate, source) = if upper == lower {
                (lower, "voted")
            } else {
                // Integer midpoint; ordering of lower/upper is guaranteed
                // (upper is a vote at-or-after, lower at-or-before), but sum
                // then halve regardless.
                ((lower + upper) / 2, "interpolated")
            };
            ResolvedTime {
                estimated_time: Some(estimate),
                time_lower: Some(lower),
                time_upper: Some(upper),
                time_source: Some(source),
            }
        }
        None => {
            // Unbounded upper: a session more recent than the last known vote.
            // The estimate is the lower bound; still "interpolated" (open
            // interval), never "voted".
            ResolvedTime {
                estimated_time: Some(lower),
                time_lower: Some(lower),
                time_upper: None,
                time_source: Some("interpolated"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(secs: i64) -> NaiveDateTime {
        chrono::DateTime::from_timestamp(secs, 0)
            .unwrap()
            .naive_utc()
    }

    #[test]
    fn observed_wins_and_is_zero_width() {
        // synced_at present -> "observed", lower == upper == estimate, even if
        // session votes exist (they're ignored).
        let r = resolve_time(Some(dt(1000)), Some(dt(500)), Some(dt(2000)));
        assert_eq!(r.time_source, Some("observed"));
        assert_eq!(r.estimated_time, Some(1000));
        assert_eq!(r.time_lower, Some(1000));
        assert_eq!(r.time_upper, Some(1000));
    }

    #[test]
    fn voted_is_zero_width_interval() {
        // lower == upper (a direct vote) -> "voted".
        let r = resolve_time(None, Some(dt(1500)), Some(dt(1500)));
        assert_eq!(r.time_source, Some("voted"));
        assert_eq!(r.estimated_time, Some(1500));
        assert_eq!(r.time_lower, Some(1500));
        assert_eq!(r.time_upper, Some(1500));
    }

    #[test]
    fn interpolated_uses_midpoint_and_keeps_bounds() {
        // upper > lower -> "interpolated", estimate is the midpoint.
        let r = resolve_time(None, Some(dt(1000)), Some(dt(2000)));
        assert_eq!(r.time_source, Some("interpolated"));
        assert_eq!(r.time_lower, Some(1000));
        assert_eq!(r.time_upper, Some(2000));
        assert_eq!(r.estimated_time, Some(1500));
    }

    #[test]
    fn interpolated_unbounded_upper_falls_back_to_lower() {
        // No upper vote yet -> estimate == lower, upper None, still interpolated.
        let r = resolve_time(None, Some(dt(1000)), None);
        assert_eq!(r.time_source, Some("interpolated"));
        assert_eq!(r.estimated_time, Some(1000));
        assert_eq!(r.time_lower, Some(1000));
        assert_eq!(r.time_upper, None);
    }

    #[test]
    fn no_information_is_all_none() {
        let r = resolve_time(None, None, None);
        assert_eq!(r.time_source, None);
        assert_eq!(r.estimated_time, None);
        assert_eq!(r.time_lower, None);
        assert_eq!(r.time_upper, None);
    }

    #[test]
    fn no_synced_no_lower_but_upper_present_is_all_none() {
        // Defensive: without a lower bound we cannot form an interval even if a
        // stray upper is passed; treat as no information.
        let r = resolve_time(None, None, Some(dt(2000)));
        assert_eq!(r.time_source, None);
        assert_eq!(r.estimated_time, None);
    }
}
