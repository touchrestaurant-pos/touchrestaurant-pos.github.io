import 'outbox_pusher.dart';
import 'pg_client.dart';

/// Sends changes directly to a local PostgreSQL database via SQL.
/// Implements the same interface as SupabaseBackend so the outbox pusher
/// works identically.
class PostgresBackend implements SyncBackend {
  PostgresBackend(this._pg);
  final PgClient _pg;

  @override
  Future<void> insert(String table, Map<String, dynamic> row) async {
    final fields = Map<String, dynamic>.from(row);
    if (fields.isEmpty) return;
    final cols = fields.keys.toList();
    final placeholders = cols.map((c) => '@$c').join(', ');
    final sql = 'INSERT INTO public."$table" (${cols.map((c) => '"$c"').join(', ')}) '
        'VALUES ($placeholders) ON CONFLICT (id) DO NOTHING';
    try {
      await _pg.execute(sql, _sanitize(fields));
    } catch (e) {
      if ('$e'.contains('duplicate key') || '$e'.contains('unique_violation')) return;
      throw RefusedException('$e');
    }
  }

  @override
  Future<bool> update(String table, dynamic id, Map<String, dynamic> fields) async {
    final data = Map<String, dynamic>.from(fields)..remove('id');
    if (data.isEmpty) return true;
    final sets = data.keys.map((c) => '"$c" = @$c').join(', ');
    final params = _sanitize(data)..['_id'] = '$id';
    final sql = 'UPDATE public."$table" SET $sets WHERE id = @_id';
    try {
      await _pg.execute(sql, params);
      return true;
    } catch (e) {
      throw RefusedException('$e');
    }
  }

  @override
  Future<void> delete(String table, dynamic id) async {
    try {
      await _pg.execute('DELETE FROM public."$table" WHERE id = @_id', {'_id': '$id'});
    } catch (e) {
      throw RefusedException('$e');
    }
  }

  /// Pull all rows from a table (used by the sync service).
  Future<List<Map<String, dynamic>>> selectAll(String table) async {
    return _trimDates(table, await _pg.query('SELECT * FROM public."$table"'));
  }

  final Map<String, List<String>> _dateCols = {};

  /// `date` columns arrive as timestamps; give them back as plain yyyy-mm-dd like the cloud does.
  Future<List<Map<String, dynamic>>> _trimDates(String table, List<Map<String, dynamic>> rows) async {
    if (rows.isEmpty) return rows;
    var cols = _dateCols[table];
    if (cols == null) {
      final info = await _pg.query(
        'SELECT column_name FROM information_schema.columns '
        "WHERE table_schema = 'public' AND table_name = @t AND data_type = 'date'",
        {'t': table},
      );
      cols = [for (final r in info) '${r['column_name']}'];
      _dateCols[table] = cols;
    }
    for (final r in rows) {
      for (final c in cols) {
        final v = r[c];
        if (v is String && v.length >= 10) r[c] = v.substring(0, 10);
      }
    }
    return rows;
  }

  /// Pull orders from the last N days.
  Future<List<Map<String, dynamic>>> selectOrders(int days) async {
    return _trimDates(
      'orders',
      await _pg.query(
        "SELECT * FROM public.orders WHERE created_at >= now() - interval '${days} days' ORDER BY created_at",
      ),
    );
  }

  /// Pull order_items or payments for a set of order ids.
  Future<List<Map<String, dynamic>>> selectByOrderIds(String table, List<String> orderIds) async {
    if (orderIds.isEmpty) return [];
    final result = <Map<String, dynamic>>[];
    // Process in chunks of 80 to match the Supabase sync pattern.
    for (var i = 0; i < orderIds.length; i += 80) {
      final chunk = orderIds.sublist(i, i + 80 > orderIds.length ? orderIds.length : i + 80);
      final placeholders = List.generate(chunk.length, (j) => '@id$j').join(', ');
      final params = <String, dynamic>{};
      for (var j = 0; j < chunk.length; j++) {
        params['id$j'] = chunk[j];
      }
      final rows = await _pg.query(
        'SELECT * FROM public."$table" WHERE order_id IN ($placeholders)',
        params,
      );
      result.addAll(rows);
    }
    return _trimDates(table, result);
  }

  /// Sanitize values for SQL parameters — convert lists to Postgres array syntax, etc.
  static Map<String, dynamic> _sanitize(Map<String, dynamic> m) {
    final out = <String, dynamic>{};
    for (final e in m.entries) {
      var v = e.value;
      if (v is List) {
        // Convert Dart list to Postgres array literal.
        v = '{${v.map((x) => '"${x.toString().replaceAll('"', '\\"')}"').join(',')}}';
      }
      out[e.key] = v;
    }
    return out;
  }
}
