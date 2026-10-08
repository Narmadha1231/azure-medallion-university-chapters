from university_chapters.transforms import apply_quality, build_gold, features_to_df


def sample_features():
    return [
        {"attributes": {"OBJECTID": 1, "ChapterID": "CA-OK", "University_Chapter": "Clean", "City": "Davis", "State": "CA"}, "geometry": {"x": -121.7, "y": 38.5}},
        {"attributes": {"OBJECTID": 2, "ChapterID": "CA-Q1", "University_Chapter": "Bad", "City": "Davis", "State": "CA"}, "geometry": {"x": 999.0, "y": 38.5}},
        {"attributes": {"OBJECTID": 3, "ChapterID": "CA-W1", "University_Chapter": "Warn", "City": "UNKNOWN", "State": "CA"}, "geometry": {"x": -121.7, "y": 38.5}},
    ]


def test_invalid_coordinates_are_quarantined_and_not_in_gold(spark):
    silver, quarantine = apply_quality(features_to_df(spark, sample_features(), "test-run"))
    gold_ids = {r.chapter_id for r in build_gold(silver).collect()}
    assert {r.chapter_id for r in quarantine.collect()} == {"CA-Q1"}
    assert "CA-Q1" not in gold_ids


def test_unknown_city_warns_and_still_publishes(spark):
    silver, _ = apply_quality(features_to_df(spark, sample_features(), "test-run"))
    row = silver.filter("chapter_id = 'CA-W1'").first()
    assert row.dq_status == "WARNING"
    assert row.dq_warnings == ["MISSING_OR_UNKNOWN_CITY"]
    assert build_gold(silver).filter("chapter_id = 'CA-W1'").count() == 1
