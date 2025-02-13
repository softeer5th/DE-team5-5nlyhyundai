-- keyword 중복체크 트리거
CREATE OR REPLACE FUNCTION unique_array()
RETURNS TRIGGER AS $$
BEGIN
    NEW.keywords = ARRAY(SELECT DISTINCT UNNEST(NEW.keywords));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ensure_unique_keywords_bobae
    BEFORE INSERT OR UPDATE ON probe_bobae
    FOR EACH ROW
    EXECUTE FUNCTION unique_array();

CREATE TRIGGER ensure_unique_keywords_clien
    BEFORE INSERT OR UPDATE ON probe_clien
    FOR EACH ROW
    EXECUTE FUNCTION unique_array();

CREATE TRIGGER ensure_unique_keywords_dcmotors
    BEFORE INSERT OR UPDATE ON probe_dcmotors
    FOR EACH ROW
    EXECUTE FUNCTION unique_array();

CREATE TRIGGER ensure_unique_keyword_keyword_set
    BEFORE INSERT OR UPDATE ON keyword_set
    FOR EACH ROW
    EXECUTE FUNCTION unique_array();