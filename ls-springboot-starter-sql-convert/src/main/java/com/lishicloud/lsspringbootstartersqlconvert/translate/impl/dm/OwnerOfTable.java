package com.lishicloud.lsspringbootstartersqlconvert.translate.impl.dm;


import com.lishicloud.lsspringbootstartersqlconvert.translate.TableNameTranslate;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;


/**
 * 表拥有者 通常需要明确的权限申明的SQL实现数据库
 *
 * @author admin
 */
@Slf4j
public class OwnerOfTable extends TableNameTranslate implements DMNorm {

    private final String owenName;
    private final Character linker;

    public OwnerOfTable(String owenName) {
        super();
        this.owenName = owenName;
        //default
        linker = '.';
        synchronized (REGISTER_LOCK) {
            if (null == tableNameNormTranslateOfUnmodifiable) {
                tableNameNormTranslateOfUnmodifiable = this;
            }
        }
    }

    public OwnerOfTable(String owenName, Character linker) {
        super();
        this.owenName = owenName;
        this.linker = linker;
    }

    @Override
    public SqlNode translate(SqlNode node) {
        SqlIdentifier sqlIdentifier = (SqlIdentifier) node;
        return new SqlIdentifier(owenName + linker + sqlIdentifier.getSimple(), sqlIdentifier.getParserPosition());
    }

    @Override
    public void destroy() {
        synchronized (DESTROY_LOCK) {
            if (tableNameNormTranslateOfUnmodifiable != null) {
                tableNameNormTranslateOfUnmodifiable = null; // Help GC, interrupt strong reference
            }
        }
        log.info(this.getClass().getName() +"destroy successful!");
    }
}
