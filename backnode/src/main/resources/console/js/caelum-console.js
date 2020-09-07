
const dt_format = "UTC:yyyy-mm-dd'T'HH:MM:ss.l'Z'";

function checkTagNameIs(element, expected_tag) {
    const actual_tag = element.prop('tagName');
    if ( actual_tag != expected_tag ) {
        throw 'Unexpected tagName of element ID ' + element.attr('id')
            + '. Expected: ' + expected_tag + ' but: ' + actual_tag;
    }
    return element;
}

function fillOptions(select_element, data_rows) {
    for ( let row = 0; row < data_rows.length; row ++ ) {
        let opt_val = data_rows[row];
        select_element.append(new Option(opt_val, opt_val, row == 0, row == 0));
    }
}

function createClear(element) {
    return function() {
        element.val('')
        return false
    }
}

// Update categories. 
// Params:
//               $ - jQuery instance.
// category_select - Element of type <SELECT> that is target for categories.
// error_container - Element to store error text in case of failures.
//        callback - Callback function in case of successful loading (optional).
// Return: false
function updateCategories($, category_select, error_container, callback) {
    $.getJSON('/api/v1/categories', function(response) {
        if ( response.error == true ) {
            error_container.text('Categories query failed: [' + response.code + '] ' + response.message)
        } else {
            category_select.empty()
            fillOptions(category_select, response.data.rows)
            if ( callback ) callback()
        }
    })
    return false
}

// Update symbols.
// Params:
//               $ - jQuery instance.
//   symbol_select - Element of type <SELECT> that is target for symbols.
// error_container - Element to store error text in case of failures.
// category_select - Element of type <SELECT> that is source of category.
//        callback - Callback function in case of successful loading (optional).
// Return: false
function updateSymbols($, symbol_select, error_container, category_select, callback) {
    const params = { category: category_select.val() }
    $.getJSON('/api/v1/symbols', params, function(response) {
        if ( response.error == true ) {
            error_container.text('Symbols query failed: [' + response.code + '] ' + response.message)
        } else {
            symbol_select.empty()
            fillOptions(symbol_select, response.data.rows)
            if ( callback ) callback()
        }
    })
    return false
}

// Update intervals.
// Params:
//               $ - jQuery instance.
// interval_select - Element of type <SELECT> that is target for intervals.
// error_container - Element to store error text in case of failures.
// Return: false
function updateIntervals($, interval_select, error_container) {
    $.getJSON('/api/v1/intervals', function(response) {
        if ( response.error == true ) {
            error_container.text('Intervals query failed: [' + response.code + '] ' + response.message)
        } else {
            interval_select.empty()
            fillOptions(interval_select, response.data.rows)
        }
    });
    return false;
}

// Create handler of symbol dropdown.
// Params:
//                  $ - jQuery instance.
//   symbol_select_id - ID of an element of type <SELECT> that is a target to fill with symbols.
// error_container_id - ID of an element used to put an error message in case of failures.
// category_select_id - ID of an element of type <SELECT> that is a source of chosen category.
//           callback - Callback function in case of successful loading (optional).
// Return: Function that updates symbols when called. That function always return false.
function createSymbolDropdown($, symbol_select_id, error_container_id, category_select_id, callback) {
    const symbol_select = checkTagNameIs($(symbol_select_id), 'SELECT')
    const category_select = checkTagNameIs($(category_select_id), 'SELECT')
    const error_container = $(error_container_id)
    return function() { return updateSymbols($, symbol_select, error_container, category_select, callback) }
}

// Create handler of category dropdown.
// Params:
//                  $ - jQuery instance.
// category_select_id - ID of an element of type <SELECT> that is a target to fill with categories.
// error_container_id - ID of an element used to put an error message in case of failures.
//           callback - Callback function in case of successful loading (optional).
// Return: Function that updates categories when called. That function always return false.
function createCategoryDropdown($, category_select_id, error_container_id, callback) {
    const category_select = checkTagNameIs($(category_select_id), 'SELECT')
    const error_container = $(error_container_id)
    return function() { return updateCategories($, category_select, error_container, callback) }
}

// Create handler of interval dropdown.
// Params:
//                  $ - jQuery instance.
// interval_select_id - ID of an element of type <SELECT> that is a target to fill with intervals.
// error_container_id - ID of an element used to put an error message in case of failures.
// Return: Function that updates intervals when called. That function always return false.
function createIntervalDropdown($, interval_select_id, error_container_id) {
    const interval_select = checkTagNameIs($(interval_select_id), 'SELECT')
    const error_container = $(error_container_id)
    return function() { return updateIntervals($, interval_select, error_container) }
}
