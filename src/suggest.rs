pub fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }

    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0; b.len() + 1];

    for (i, ca) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            cur[j + 1] = std::cmp::min(std::cmp::min(cur[j] + 1, prev[j + 1] + 1), prev[j] + cost);
        }
        prev.clone_from_slice(&cur);
    }

    prev[b.len()]
}

/// Return the closest candidate within `max_distance`, preferring lower distance.
pub fn best_match<'a>(
    needle: &str,
    candidates: &'a [String],
    max_distance: usize,
) -> Option<&'a str> {
    let mut best: Option<(&str, usize)> = None;
    for candidate in candidates {
        let dist = levenshtein(needle, candidate);
        let current_best = best.map(|(_, d)| d).unwrap_or(usize::MAX);
        if dist < current_best {
            best = Some((candidate.as_str(), dist));
        }
    }

    match best {
        Some((cand, dist)) if dist > 0 && dist <= max_distance => Some(cand),
        _ => None,
    }
}
